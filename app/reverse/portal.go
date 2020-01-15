// +build !confonly

package reverse

import (
	"context"
	"io"
	"sync"
	"time"

	"v2ray.com/core/common"
	"v2ray.com/core/common/buf"
	"v2ray.com/core/common/errors"
	"v2ray.com/core/common/log"
	"v2ray.com/core/common/mux"
	"v2ray.com/core/common/net"
	"v2ray.com/core/common/session"
	"v2ray.com/core/common/task"
	"v2ray.com/core/features/outbound"
	"v2ray.com/core/transport"
)

type Portal struct {
	ohm     outbound.Manager
	tag     string
	domain  string
	control control
}

func NewPortal(config *PortalConfig, ohm outbound.Manager) (*Portal, error) {
	if config.Tag == "" {
		return nil, newError("portal tag is empty")
	}

	if config.Domain == "" {
		return nil, newError("portal domain is empty")
	}

	return &Portal{
		ohm:    ohm,
		tag:    config.Tag,
		domain: config.Domain,
	}, nil
}

func (p *Portal) Start() error {
	return p.ohm.AddHandler(context.Background(), &Outbound{
		portal: p,
		tag:    p.tag,
	})
}

func (p *Portal) Close() error {
	return p.ohm.RemoveHandler(context.Background(), p.tag)
}

func (p *Portal) HandleConnection(ctx context.Context, link *transport.Link) error {
	outboundMeta := session.OutboundFromContext(ctx)
	if outboundMeta == nil {
		return newError("outbound metadata not found").AtError()
	}

	if isDomain(outboundMeta.Target, p.domain) {
		// control connection
		if outboundMeta.Target.Port == net.Port(1) {
			p.control.link = link
			return p.control.run(ctx)
		}
		// data connection
		return p.control.handleDataConn(ctx, link, outboundMeta.Target.Port.Value())
	}
	// new request via control
	return p.control.Dispatch(ctx, link)
}

type Outbound struct {
	portal *Portal
	tag    string
}

func (o *Outbound) Tag() string {
	return o.tag
}

func (o *Outbound) Dispatch(ctx context.Context, link *transport.Link) {
	if err := o.portal.HandleConnection(ctx, link); err != nil {
		newError("failed to process reverse connection").Base(err).WriteToLog(session.ExportIDToError(ctx))
		common.Interrupt(link.Writer)
	}
}

func (o *Outbound) Start() error {
	return nil
}

func (o *Outbound) Close() error {
	return nil
}

type control struct {
	link    *transport.Link
	pending [65535]*transport.Link
	mutex   sync.RWMutex
}

func (c *control) Dispatch(ctx context.Context, link *transport.Link) error {
	dest := session.OutboundFromContext(ctx).Target
	c.mutex.Lock()
	defer c.mutex.Unlock()
	id := uint16(2)
	// find available id
	for ; id < uint16(65535); id++ {
		if c.pending[id] == nil {
			break
		}
	}
	// Initialize connection via Control link
	b := buf.New()
	meta := mux.FrameMetadata{
		SessionStatus: mux.SessionStatusNew,
		SessionID:     id,
		Target:        dest,
	}
	if err := meta.WriteTo(b); err != nil {
		return err
	}
	err := c.link.Writer.WriteMultiBuffer(buf.MultiBuffer{b})
	if err != nil {
		return err
	}
	c.pending[id] = link
	return nil
}

func (c *control) handleDataConn(ctx context.Context, bridgeLink *transport.Link, sessionID uint16) error {
	// Take pending connection out
	c.mutex.Lock()
	link := c.pending[sessionID]
	c.pending[sessionID] = nil
	c.mutex.Unlock()
	if link == nil {
		return newError("cannot find corresponding sessionID")
	}
	// Copy
	b2cFunc := func() error {
		return buf.Copy(bridgeLink.Reader, link.Writer)
	}
	c2bFunc := func() error {
		return buf.Copy(link.Reader, bridgeLink.Writer)
	}
	b2cDonePost := task.OnSuccess(b2cFunc, task.Close(link.Writer))
	c2bDonePost := task.OnSuccess(c2bFunc, task.Close(bridgeLink.Writer))
	err := task.Run(ctx, b2cDonePost, c2bDonePost)
	if err != nil {
		common.Interrupt(link.Writer)
		common.Interrupt(bridgeLink.Writer)
		return newError("connection closed").Base(err)
	}
	return nil
}

func (c *control) run(ctx context.Context) error {
	timer := time.NewTicker(15 * time.Second)
	defer timer.Stop()
	keepalive := buf.StackNew()
	defer keepalive.Release()
	meta := mux.FrameMetadata{SessionStatus: mux.SessionStatusKeepAlive}
	common.Must(meta.WriteTo(&keepalive))

	reader := &buf.BufferedReader{Reader: c.link.Reader}
	var err error
	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		case <- timer.C:
			err = c.link.Writer.WriteMultiBuffer(buf.MultiBuffer{&keepalive})
			break
		default:
			err = c.handleFrame(ctx, reader)
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
			common.Close(c.link.Writer)
			common.Interrupt(c.link.Reader)
			return newError("unexpected EOF").Base(err)
		}
	}
}

func (c *control) handleFrame(ctx context.Context, r *buf.BufferedReader) error {
	var meta mux.FrameMetadata
	err := meta.Unmarshal(r)
	if err != nil {
		return newError("failed to read metadata").Base(err).AtWarning()
	}

	switch meta.SessionStatus {
	case mux.SessionStatusKeepAlive:
		err = c.handleStatusKeepAlive(&meta, r)
	case mux.SessionStatusEnd:
		err = c.handleStatusEnd(&meta, r)
	default:
		status := meta.SessionStatus
		return newError("unknown status: ", status).AtError()
	}
	if err != nil {
		return newError("failed to process data").Base(err)
	}
	return nil
}

func (c *control) handleStatusKeepAlive(meta *mux.FrameMetadata, r *buf.BufferedReader) error {
	if meta.Option.Has(mux.OptionData) {
		return buf.Copy(mux.NewStreamReader(r), buf.Discard)
	}
	return nil
}

func (c *control) handleStatusEnd(meta *mux.FrameMetadata, r *buf.BufferedReader) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if link := c.pending[meta.SessionID]; link != nil {
		common.Interrupt(link.Writer)
		common.Close(link.Reader)
	}
	if meta.Option.Has(mux.OptionData) {
		return buf.Copy(mux.NewStreamReader(r), buf.Discard)
	}
	return nil
}
