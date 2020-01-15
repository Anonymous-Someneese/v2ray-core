// +build !confonly

package reverse

import (
	"context"
	"time"
	"io"

	"v2ray.com/core/common"
	"v2ray.com/core/common/buf"
	"v2ray.com/core/common/errors"
	"v2ray.com/core/common/log"
	"v2ray.com/core/common/mux"
	"v2ray.com/core/common/net"
	"v2ray.com/core/common/session"
	"v2ray.com/core/common/task"
	"v2ray.com/core/features/routing"
	"v2ray.com/core/transport"
)

// Bridge is a component in reverse proxy, that relays connections from Portal to local address.
type Bridge struct {
	dispatcher  routing.Dispatcher
	tag         string
	domain      string
	cancel      context.CancelFunc
}

// NewBridge creates a new Bridge instance.
func NewBridge(config *BridgeConfig, dispatcher routing.Dispatcher) (*Bridge, error) {
	if config.Tag == "" {
		return nil, newError("bridge tag is empty")
	}
	if config.Domain == "" {
		return nil, newError("bridge domain is empty")
	}

	b := &Bridge{
		dispatcher: dispatcher,
		tag:        config.Tag,
		domain:     config.Domain,
	}
	return b, nil
}

func (b *Bridge) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.run(ctx)
}

func (b *Bridge) Close() {
	if b.cancel != nil {
		b.cancel()
		b.cancel = nil
	}
}

func (b *Bridge) run(ctx context.Context) {
	for {
		link, err := b.dispatcher.Dispatch(ctx, net.Destination{
			Network: net.Network_TCP,
			Address: net.DomainAddress(b.domain),
			Port:    1,
		})
		if err != nil {
			if errors.Cause(err) != context.Canceled {
				newError("failed to establish control connection").AtWarning().Base(err).WriteToLog(session.ExportIDToError(ctx))
				continue
			}
			newError("task stopped").Base(err).WriteToLog(session.ExportIDToError(ctx))
			break
		}
		if err = b.handleLink(ctx, link); err != nil {
			newError("connection closed").Base(err).WriteToLog(session.ExportIDToError(ctx))
		}
	}
}

func (b *Bridge) handleLink(ctx context.Context, link *transport.Link) error {
	timer := time.NewTicker(15 * time.Second)
	defer timer.Stop()
	keepalive := buf.StackNew()
	defer keepalive.Release()
	meta := mux.FrameMetadata{SessionStatus: mux.SessionStatusKeepAlive}
	common.Must(meta.WriteTo(&keepalive))

	reader := &buf.BufferedReader{Reader: link.Reader}
	var err error
	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		case <- timer.C:
			err = link.Writer.WriteMultiBuffer(buf.MultiBuffer{&keepalive})
			break
		default:
			err = b.handleFrame(ctx, reader, link.Writer)
			if err != nil && errors.GetSeverity(err) >= log.Severity_Info {
				newError("handle frame error").Base(err).WriteToLog(session.ExportIDToError(ctx))
				err = nil
			}
			break
		}
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return err
			}
			common.Close(link.Writer)
			common.Interrupt(link.Reader)
			return newError("unexpected EOF").Base(err)
		}
	}
}

func (b *Bridge) handleFrame(ctx context.Context, r *buf.BufferedReader, w buf.Writer) error {
	var meta mux.FrameMetadata
	err := meta.Unmarshal(r)
	if err != nil {
		return newError("failed to read metadata").Base(err).AtWarning()
	}

	switch meta.SessionStatus {
	case mux.SessionStatusKeepAlive:
		err = b.handleStatusKeepAlive(&meta, r)
	case mux.SessionStatusNew:
		err = b.handleStatusNew(ctx, &meta, r, w)
	default:
		status := meta.SessionStatus
		return newError("unknown status: ", status).AtError()
	}
	if err != nil {
		return newError("failed to process data").Base(err)
	}
	return nil
}

func (b *Bridge) handleStatusKeepAlive(meta *mux.FrameMetadata, r *buf.BufferedReader) error {
	if meta.Option.Has(mux.OptionData) {
		return buf.Copy(mux.NewStreamReader(r), buf.Discard)
	}
	return nil
}

func (b *Bridge) handleStatusNew(ctx context.Context, meta *mux.FrameMetadata, r *buf.BufferedReader, w buf.Writer) error {
	newError("received request for ", meta.Target).WriteToLog(session.ExportIDToError(ctx))
	msg := &log.AccessMessage{
		To:     meta.Target,
		Status: log.AccessAccepted,
		Reason: "",
	}
	if inbound := session.InboundFromContext(ctx); inbound != nil && inbound.Source.IsValid() {
		msg.From = inbound.Source
		msg.Email = inbound.User.Email
	}
	ctxmsg := log.ContextWithAccessMessage(ctx, msg)

	type linkOut struct {
		*transport.Link
		err error
	}
	portalChan := make(chan linkOut)
	defer close(portalChan)
	targetChan := make(chan linkOut)
	defer close(targetChan)
	// connect to portal
	go func() {
		if meta.SessionID < 2 {
			portalChan <- linkOut{Link: nil, err: newError("invalid session id")}
			return
		}
		link, err := b.dispatcher.Dispatch(ctx, net.Destination{
			Network: net.Network_TCP,
			Address: net.DomainAddress(b.domain),
			Port:    net.Port(meta.SessionID),
		})
		portalChan <- linkOut{Link: link, err: err}
	}()
	// connect to target
	go func() {
		link, err := b.dispatcher.Dispatch(ctxmsg, meta.Target)
		targetChan <- linkOut{Link: link, err: err}
	}()
	portalLink := <- portalChan
	targetLink := <- targetChan
	if portalLink.err != nil {
		common.Close(targetLink.Link)
		if meta.Option.Has(mux.OptionData) {
			buf.Copy(mux.NewStreamReader(r), buf.Discard)
		}
		// inform portal the connection is failed
		closing := buf.New()
		mux.FrameMetadata{
			SessionStatus: mux.SessionStatusEnd,
			SessionID:     meta.SessionID,
		}.WriteTo(closing)
		if err := w.WriteMultiBuffer(buf.MultiBuffer{closing}); err != nil {
			return newError("failed to write error").AtWarning().Base(err)
		}
		return newError("failed to connect to portal.").Base(portalLink.err)
	}
	if targetLink.err != nil {
		common.Close(portalLink.Link)
		if meta.Option.Has(mux.OptionData) {
			buf.Copy(mux.NewStreamReader(r), buf.Discard)
		}
		return newError("failed to dispatch request.").Base(targetLink.err)
	}
	// Copy optiondata
	if meta.Option.Has(mux.OptionData) {
		var rr buf.Reader
		if meta.Target.Network == net.Network_UDP {
			rr = mux.NewPacketReader(r)
		} else {
			rr = mux.NewStreamReader(r)
		}
		if err := buf.Copy(rr, targetLink.Writer); err != nil {
			buf.Copy(rr, buf.Discard)
			common.Close(targetLink.Link)
			common.Interrupt(portalLink.Link)
			return newError("failed copy OptionData.").Base(err)
		}
	}
	// Copy
	t2pFunc := func() error {
		return buf.Copy(targetLink.Reader, portalLink.Writer)
	}
	p2tFunc := func() error {
		return buf.Copy(portalLink.Reader, targetLink.Writer)
	}
	t2pDonePost := task.OnSuccess(t2pFunc, task.Close(portalLink.Writer))
	p2tDonePost := task.OnSuccess(p2tFunc, task.Close(targetLink.Writer))
	go func() {
		err := task.Run(ctx, t2pDonePost, p2tDonePost)
		if err != nil {
			common.Interrupt(portalLink.Writer)
			common.Interrupt(targetLink.Writer)
			newError("connection closed").Base(err).WriteToLog(session.ExportIDToError(ctx))
		}
	}()
	return nil
}
