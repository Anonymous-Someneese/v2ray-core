// +build !confonly

package reverse

import (
	"context"
	"io"
	"time"

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
	dispatcher routing.Dispatcher
	tag        string
	domain     string
	cancel     context.CancelFunc
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
	newError("establish control link").AtDebug().WriteToLog(session.ExportIDToError(ctx))
	link.Writer.WriteMultiBuffer(buf.MultiBuffer{&keepalive})

	reader := &buf.BufferedReader{Reader: link.Reader}
	var err error
	for {
		select {
		case <-ctx.Done():
			common.Close(link.Writer)
			common.Close(reader)
			return context.Canceled
		case <-timer.C:
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

	if meta.Target.Network == net.Network_Unknown {
		if meta.Option.Has(mux.OptionData) {
			buf.Copy(mux.NewPacketReader(r), buf.Discard)
		}
		return newError("Network is unknown")
	}
	// Read optiondata
	var optiondata buf.MultiBuffer
	if meta.Option.Has(mux.OptionData) {
		var rr buf.Reader
		if meta.Target.Network == net.Network_TCP {
			rr = mux.NewStreamReader(r)
		} else {
			rr = mux.NewPacketReader(r)
		}
		// Read all
		for {
			mb, err := rr.ReadMultiBuffer()
			if err == io.EOF {
				break
			}
			if err != nil {
				buf.Copy(rr, buf.Discard)
				// inform portal the connection is failed
				closing := mux.FrameMetadata{
					SessionStatus: mux.SessionStatusEnd,
					SessionID:     meta.SessionID,
				}
				if err := writeFrame(w, &closing); err != nil {
					return newError("failed to write error").AtWarning().Base(err)
				}
				return newError("failed copy OptionData.").Base(err)
			}
			optiondata = append(optiondata, mb...)
		}
	}
	go b.handleNewRequest(ctx, ctxmsg, meta, optiondata, w)
	return nil
}

func (b *Bridge) handleNewRequest(ctx, ctxmsg context.Context, meta *mux.FrameMetadata, optiondata buf.MultiBuffer, w buf.Writer) {
	var portalLink *transport.Link
	var targetLink *transport.Link
	var err error
	// connect to portal
	if meta.SessionID < 2 {
		err = newError("invalid session id")
	}
	portalLink, err = b.dispatcher.Dispatch(ctx, net.Destination{
		Network: meta.Target.Network,
		Address: net.DomainAddress(b.domain),
		Port:    net.Port(meta.SessionID),
	})
	if err != nil {
		// inform portal the connection is failed
		closing := mux.FrameMetadata{
			SessionStatus: mux.SessionStatusEnd,
			SessionID:     meta.SessionID,
		}
		if err := writeFrame(w, &closing); err != nil {
			common.Interrupt(w)
			newError("failed to write error").Base(err).WriteToLog(session.ExportIDToError(ctx))
		}
		newError("failed to connect to portal.").Base(err).WriteToLog(session.ExportIDToError(ctx))
	}
	// connect to target
	targetLink, err = b.dispatcher.Dispatch(ctxmsg, meta.Target)
	if err != nil {
		common.Close(portalLink.Reader)
		common.Close(portalLink.Writer)
		newError("failed to dispatch request.").Base(err).WriteToLog(session.ExportIDToError(ctx))
	}
	// Copy
	t2pFunc := func() error {
		return buf.Copy(targetLink.Reader, portalLink.Writer)
	}
	p2tFunc := func() error {
		newError("writting {} bytes to target", optiondata.Len()).AtDebug().WriteToLog(session.ExportIDToError(ctx))
		if err := targetLink.Writer.WriteMultiBuffer(optiondata); err != nil {
			return err
		}
		optiondata = nil
		return buf.Copy(portalLink.Reader, targetLink.Writer)
	}
	t2pDonePost := task.OnSuccess(t2pFunc, task.Close(portalLink.Writer))
	p2tDonePost := task.OnSuccess(p2tFunc, task.Close(targetLink.Writer))
	err = task.Run(ctxmsg, t2pDonePost, p2tDonePost)
	if err != nil {
		common.Interrupt(portalLink.Writer)
		common.Interrupt(targetLink.Writer)
		newError("connection closed").Base(err).WriteToLog(session.ExportIDToError(ctx))
	}
}

func writeFrame(w buf.Writer, f *mux.FrameMetadata) error {
	buffer := buf.New()
	if err := f.WriteTo(buffer); err != nil {
		buffer.Release()
		return err
	}
	if err := w.WriteMultiBuffer(buf.MultiBuffer{buffer}); err != nil {
		return err
	}
	return nil
}
