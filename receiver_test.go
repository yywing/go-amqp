package amqp

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-amqp/pkg/encoding"
	"github.com/Azure/go-amqp/pkg/fake"
	"github.com/Azure/go-amqp/pkg/frames"
	"github.com/Azure/go-amqp/pkg/test"
	"github.com/stretchr/testify/require"
)

func TestReceiverInvalidOptions(t *testing.T) {
	conn := fake.NewNetConn(receiverFrameHandlerNoUnhandled(0, ReceiverSettleModeFirst))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleMode(3).Ptr(),
	})
	cancel()
	require.Error(t, err)
	require.Nil(t, r)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err = session.NewReceiver(ctx, "source", &ReceiverOptions{
		Durability: Durability(3),
	})
	cancel()
	require.Error(t, err)
	require.Nil(t, r)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err = session.NewReceiver(ctx, "source", &ReceiverOptions{
		ExpiryPolicy: ExpiryPolicy("not-a-real-policy"),
	})
	cancel()
	require.Error(t, err)
	require.Nil(t, r)
}

func TestReceiverMethodsNoReceive(t *testing.T) {
	const linkName = "test"
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		switch ff := req.(type) {
		case *fake.AMQPProto:
			return newResponse(fake.ProtoHeader(fake.ProtoAMQP))
		case *frames.PerformOpen:
			return newResponse(fake.PerformOpen("test"))
		case *frames.PerformBegin:
			return newResponse(fake.PerformBegin(0, remoteChannel))
		case *frames.PerformAttach:
			require.Equal(t, DurabilityUnsettledState, ff.Target.Durable)
			require.Equal(t, ExpiryPolicyNever, ff.Target.ExpiryPolicy)
			require.Equal(t, uint32(300), ff.Target.Timeout)
			return newResponse(fake.ReceiverAttach(0, linkName, 0, ReceiverSettleModeFirst, nil))
		case *frames.PerformFlow, *fake.KeepAlive:
			return fake.Response{}, nil
		case *frames.PerformDetach:
			return newResponse(fake.PerformDetach(0, ff.Handle, nil))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	const sourceAddr = "thesource"
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, sourceAddr, &ReceiverOptions{
		Name:          linkName,
		Durability:    DurabilityUnsettledState,
		ExpiryPolicy:  ExpiryPolicyNever,
		ExpiryTimeout: 300,
	})
	cancel()
	require.NoError(t, err)
	require.Equal(t, sourceAddr, r.Address())
	require.Equal(t, linkName, r.LinkName())
	require.Nil(t, r.LinkSourceFilterValue("nofilter"))
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, r.Close(ctx))
	cancel()
}

func TestReceiverLinkSourceFilter(t *testing.T) {
	conn := fake.NewNetConn(receiverFrameHandlerNoUnhandled(0, ReceiverSettleModeFirst))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	const filterName = "myfilter"
	const filterExp = "filter_exp"
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "ignored", &ReceiverOptions{
		DynamicAddress: true,
		Filters: []LinkFilter{
			NewLinkFilter(filterName, 0, filterExp),
		},
	})
	cancel()
	require.NoError(t, err)
	require.Equal(t, "test", r.Address())
	require.NotEmpty(t, r.LinkName())
	require.Equal(t, filterExp, r.LinkSourceFilterValue(filterName))
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, r.Close(ctx))
	cancel()
}

func TestReceiverOnClosed(t *testing.T) {
	conn := fake.NewNetConn(receiverFrameHandlerNoUnhandled(0, ReceiverSettleModeFirst))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background(), nil)
		errChan <- err
	}()

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, r.Close(ctx))
	cancel()
	var linkErr *LinkError
	require.ErrorAs(t, <-errChan, &linkErr)
	_, err = r.Receive(context.Background(), nil)
	require.ErrorAs(t, err, &linkErr)
	var amqpErr *Error
	// there should be no inner error when closed on our side
	require.False(t, errors.As(err, &amqpErr))
}

func TestReceiverOnSessionClosed(t *testing.T) {
	conn := fake.NewNetConn(receiverFrameHandlerNoUnhandled(0, ReceiverSettleModeFirst))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background(), nil)
		errChan <- err
	}()

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, session.Close(ctx))
	cancel()
	var sessionErr *SessionError
	require.ErrorAs(t, <-errChan, &sessionErr)
	_, err = r.Receive(context.Background(), nil)
	require.ErrorAs(t, err, &sessionErr)
}

func TestReceiverOnConnClosed(t *testing.T) {
	conn := fake.NewNetConn(receiverFrameHandlerNoUnhandled(0, ReceiverSettleModeFirst))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background(), nil)
		errChan <- err
	}()

	require.NoError(t, client.Close())
	err = <-errChan
	var connErr *ConnError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	_, err = r.Receive(context.Background(), nil)
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
}

func TestReceiverOnDetached(t *testing.T) {
	conn := fake.NewNetConn(receiverFrameHandlerNoUnhandled(0, ReceiverSettleModeFirst))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background(), nil)
		errChan <- err
	}()

	// initiate a server-side detach
	const (
		errcon  = "detaching"
		errdesc = "server side detach"
	)
	b, err := fake.PerformDetach(0, 0, &Error{Condition: errcon, Description: errdesc})
	require.NoError(t, err)
	conn.SendFrame(b)

	var linkErr *LinkError
	require.ErrorAs(t, <-errChan, &linkErr)
	require.Equal(t, ErrCond(errcon), linkErr.RemoteErr.Condition)
	require.Equal(t, errdesc, linkErr.RemoteErr.Description)
	require.NoError(t, client.Close())
	_, err = r.Receive(context.Background(), nil)
	require.ErrorAs(t, err, &linkErr)
}

func TestReceiverCloseTimeout(t *testing.T) {
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return newResponse(fake.ProtoHeader(fake.ProtoAMQP))
		case *frames.PerformOpen:
			return newResponse(fake.PerformOpen("container"))
		case *frames.PerformBegin:
			return newResponse(fake.PerformBegin(0, remoteChannel))
		case *frames.PerformEnd:
			return newResponse(fake.PerformEnd(0, nil))
		case *frames.PerformAttach:
			return newResponse(fake.ReceiverAttach(0, tt.Name, tt.Handle, ReceiverSettleModeSecond, nil))
		case *frames.PerformDetach:
			b, err := fake.PerformDetach(0, tt.Handle, nil)
			if err != nil {
				return fake.Response{}, err
			}
			// include a write delay to trigger sender close timeout
			return fake.Response{Payload: b, WriteDelay: 1 * time.Second}, nil
		case *frames.PerformFlow, *fake.KeepAlive:
			return fake.Response{}, nil
		case *frames.PerformClose:
			return newResponse(fake.PerformClose(nil))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	netConn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, netConn, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = r.Close(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = r.Close(ctx)
	cancel()
	var linkErr *LinkError
	require.ErrorAs(t, err, &linkErr)
	require.Contains(t, linkErr.Error(), context.DeadlineExceeded.Error())
	require.NoError(t, client.Close())
}

func TestReceiveInvalidMessage(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return newResponse(fake.ProtoHeader(fake.ProtoAMQP))
		case *frames.PerformOpen:
			return newResponse(fake.PerformOpen("container"))
		case *frames.PerformClose:
			return newResponse(fake.PerformClose(nil))
		case *frames.PerformBegin:
			return newResponse(fake.PerformBegin(0, remoteChannel))
		case *frames.PerformEnd:
			return newResponse(fake.PerformEnd(0, nil))
		case *frames.PerformAttach:
			return newResponse(fake.ReceiverAttach(0, tt.Name, 0, ReceiverSettleModeFirst, tt.Source.Filter))
		case *frames.PerformDetach:
			require.NotNil(t, tt.Error)
			require.EqualValues(t, ErrCondNotAllowed, tt.Error.Condition)
			return newResponse(fake.PerformDetach(0, 0, nil))
		case *frames.PerformFlow, *fake.KeepAlive:
			return fake.Response{}, nil
		case *frames.PerformDisposition:
			return newResponse(fake.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{}))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	msgChan := make(chan *Message)
	errChan := make(chan error)
	go func() {
		msg, err := r.Receive(context.Background(), nil)
		msgChan <- msg
		errChan <- err
	}()

	// missing DeliveryID
	fr, err := fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformTransfer{
		Handle: linkHandle,
	})
	require.NoError(t, err)
	conn.SendFrame(fr)

	require.Nil(t, <-msgChan)
	var linkErr *LinkError
	require.ErrorAs(t, <-errChan, &linkErr)
	require.Contains(t, linkErr.Error(), ErrCondNotAllowed)

	_, err = r.Receive(context.Background(), nil)
	require.ErrorAs(t, err, &linkErr)

	// missing MessageFormat
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err = session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)
	go func() {
		msg, err := r.Receive(context.Background(), nil)
		msgChan <- msg
		errChan <- err
	}()
	fr, err = fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformTransfer{
		DeliveryID: &deliveryID,
		Handle:     linkHandle,
	})
	require.NoError(t, err)
	conn.SendFrame(fr)

	require.Nil(t, <-msgChan)
	require.ErrorAs(t, <-errChan, &linkErr)
	require.Contains(t, linkErr.Error(), ErrCondNotAllowed)

	_, err = r.Receive(context.Background(), nil)
	require.ErrorAs(t, err, &linkErr)

	// missing delivery tag
	format := uint32(0)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err = session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)
	go func() {
		msg, err := r.Receive(context.Background(), nil)
		msgChan <- msg
		errChan <- err
	}()
	fr, err = fake.EncodeFrame(frames.TypeAMQP, 0, &frames.PerformTransfer{
		DeliveryID:    &deliveryID,
		Handle:        linkHandle,
		MessageFormat: &format,
	})
	require.NoError(t, err)
	conn.SendFrame(fr)

	require.Nil(t, <-msgChan)
	require.ErrorAs(t, <-errChan, &linkErr)
	require.Contains(t, linkErr.Error(), ErrCondNotAllowed)

	_, err = r.Receive(context.Background(), nil)
	require.ErrorAs(t, err, &linkErr)

	require.NoError(t, client.Close())
}

func TestReceiveSuccessReceiverSettleModeFirst(t *testing.T) {
	muxSem := test.NewMuxSemaphore(2)

	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		resp, err := receiverFrameHandler(0, ReceiverSettleModeFirst)(remoteChannel, req)
		if resp.Payload != nil || err != nil {
			return resp, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return newResponse(fake.PerformTransfer(0, linkHandle, deliveryID, []byte("hello")))
			}
			// ignore future flow frames as we have no response
			return fake.Response{}, nil
		case *frames.PerformDisposition:
			return fake.Response{}, nil
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := newReceiverForSession(ctx, session, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeFirst.Ptr(),
	}, receiverTestHooks{MuxSelect: muxSem.OnLoop})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx, nil)
	cancel()
	require.NoError(t, err)
	muxSem.Wait()
	if c := r.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// link credit should be 0
	if c := r.l.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(1)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	require.NoError(t, err)
	muxSem.Wait()
	if c := r.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// link credit should be 1
	if c := r.l.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(-1)
	// subsequent dispositions should have no effect
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestReceiveSuccessReceiverSettleModeSecondAccept(t *testing.T) {
	muxSem := test.NewMuxSemaphore(2)

	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		resp, err := receiverFrameHandler(0, ReceiverSettleModeSecond)(remoteChannel, req)
		if resp.Payload != nil || err != nil {
			return resp, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return newResponse(fake.PerformTransfer(0, linkHandle, deliveryID, []byte("hello")))
			}
			// ignore future flow frames as we have no response
			return fake.Response{}, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateAccepted); !ok {
				return fake.Response{}, fmt.Errorf("unexpected State %T", ff.State)
			}
			return newResponse(fake.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{}))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := newReceiverForSession(ctx, session, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeSecond.Ptr(),
	}, receiverTestHooks{MuxSelect: muxSem.OnLoop})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx, nil)
	cancel()
	require.NoError(t, err)
	if c := r.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	muxSem.Wait()
	// link credit must be zero since we only started with 1
	if c := r.l.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(2)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	require.NoError(t, err)
	muxSem.Wait()
	if c := r.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	require.Equal(t, true, msg.settled)
	// link credit should be back to 1
	if c := r.l.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(-1)
	// subsequent dispositions should have no effect
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestReceiveSuccessReceiverSettleModeSecondAcceptOnClosedLink(t *testing.T) {
	muxSem := test.NewMuxSemaphore(2)

	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		resp, err := receiverFrameHandler(0, ReceiverSettleModeSecond)(remoteChannel, req)
		if resp.Payload != nil || err != nil {
			return resp, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return newResponse(fake.PerformTransfer(0, linkHandle, deliveryID, []byte("hello")))
			}
			// ignore future flow frames as we have no response
			return fake.Response{}, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateAccepted); !ok {
				return fake.Response{}, fmt.Errorf("unexpected State %T", ff.State)
			}
			return newResponse(fake.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{}))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := newReceiverForSession(ctx, session, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeSecond.Ptr(),
	}, receiverTestHooks{MuxSelect: muxSem.OnLoop})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx, nil)
	cancel()
	require.NoError(t, err)
	muxSem.Wait()
	if c := r.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// link credit must be zero since we only started with 1
	if c := r.l.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}

	muxSem.Release(-1)
	require.NoError(t, r.Close(context.Background()))

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	var linkErr *LinkError
	require.ErrorAs(t, err, &linkErr)
}

func TestReceiveSuccessReceiverSettleModeSecondReject(t *testing.T) {
	muxSem := test.NewMuxSemaphore(2)

	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		resp, err := receiverFrameHandler(0, ReceiverSettleModeSecond)(remoteChannel, req)
		if resp.Payload != nil || err != nil {
			return resp, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return newResponse(fake.PerformTransfer(0, linkHandle, deliveryID, []byte("hello")))
			}
			// ignore future flow frames as we have no response
			return fake.Response{}, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateRejected); !ok {
				return fake.Response{}, fmt.Errorf("unexpected State %T", ff.State)
			}
			return newResponse(fake.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateRejected{}))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := newReceiverForSession(ctx, session, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeSecond.Ptr(),
	}, receiverTestHooks{MuxSelect: muxSem.OnLoop})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx, nil)
	cancel()
	require.NoError(t, err)
	muxSem.Wait()
	if c := r.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// link credit must be zero since we only started with 1
	if c := r.l.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(2)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.RejectMessage(ctx, msg, nil)
	cancel()
	require.NoError(t, err)
	muxSem.Wait()
	if c := r.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// link credit should be back to 1
	if c := r.l.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(-1)
	require.NoError(t, client.Close())
}

func TestReceiveSuccessReceiverSettleModeSecondRelease(t *testing.T) {
	muxSem := test.NewMuxSemaphore(2)

	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		resp, err := receiverFrameHandler(0, ReceiverSettleModeSecond)(remoteChannel, req)
		if resp.Payload != nil || err != nil {
			return resp, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return newResponse(fake.PerformTransfer(0, linkHandle, deliveryID, []byte("hello")))
			}
			// ignore future flow frames as we have no response
			return fake.Response{}, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateReleased); !ok {
				return fake.Response{}, fmt.Errorf("unexpected State %T", ff.State)
			}
			return newResponse(fake.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateReleased{}))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := newReceiverForSession(ctx, session, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeSecond.Ptr(),
	}, receiverTestHooks{MuxSelect: muxSem.OnLoop})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx, nil)
	cancel()
	require.NoError(t, err)
	muxSem.Wait()
	if c := r.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// link credit must be zero since we only started with 1
	if c := r.l.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(2)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.ReleaseMessage(ctx, msg)
	cancel()
	require.NoError(t, err)
	muxSem.Wait()
	if c := r.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// link credit should be back to 1
	if c := r.l.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(-1)
	require.NoError(t, client.Close())
}

func TestReceiveSuccessReceiverSettleModeSecondModify(t *testing.T) {
	muxSem := test.NewMuxSemaphore(2)

	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		resp, err := receiverFrameHandler(0, ReceiverSettleModeSecond)(remoteChannel, req)
		if resp.Payload != nil || err != nil {
			return resp, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return newResponse(fake.PerformTransfer(0, linkHandle, deliveryID, []byte("hello")))
			}
			// ignore future flow frames as we have no response
			return fake.Response{}, nil
		case *frames.PerformDisposition:
			var mod *encoding.StateModified
			var ok bool
			if mod, ok = ff.State.(*encoding.StateModified); !ok {
				return fake.Response{}, fmt.Errorf("unexpected State %T", ff.State)
			}
			if v := mod.MessageAnnotations["some"]; v != "value" {
				return fake.Response{}, fmt.Errorf("unexpected annotation value %v", v)
			}
			return newResponse(fake.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateModified{}))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := newReceiverForSession(ctx, session, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeSecond.Ptr(),
	}, receiverTestHooks{MuxSelect: muxSem.OnLoop})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx, nil)
	cancel()
	require.NoError(t, err)
	muxSem.Wait()
	if c := r.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// link credit must be zero since we only started with 1
	if c := r.l.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(2)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.ModifyMessage(ctx, msg, &ModifyMessageOptions{
		UndeliverableHere: true,
		Annotations: Annotations{
			"some": "value",
		},
	})
	cancel()
	require.NoError(t, err)
	muxSem.Wait()
	if c := r.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// link credit should be back to 1
	if c := r.l.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(-1)
	require.NoError(t, client.Close())
}

func TestReceiverPrefetch(t *testing.T) {
	conn := fake.NewNetConn(receiverFrameHandlerNoUnhandled(0, ReceiverSettleModeFirst))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	msg := r.Prefetched()
	require.Nil(t, msg)

	// now send a transfer
	b, err := fake.PerformTransfer(0, 0, 1, []byte("message 1"))
	require.NoError(t, err)
	conn.SendFrame(b)

	// wait for the transfer to "arrive"
	time.Sleep(time.Second)

	msg = r.Prefetched()
	require.NotNil(t, msg)

	msg = r.Prefetched()
	require.Nil(t, msg)

	require.NoError(t, client.Close())
}

func TestReceiveMultiFrameMessageSuccess(t *testing.T) {
	muxSem := test.NewMuxSemaphore(4)

	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		resp, err := receiverFrameHandler(0, ReceiverSettleModeSecond)(remoteChannel, req)
		if resp.Payload != nil || err != nil {
			return resp, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow, *fake.KeepAlive:
			return fake.Response{}, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateAccepted); !ok {
				return fake.Response{}, fmt.Errorf("unexpected State %T", ff.State)
			}
			return newResponse(fake.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{}))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := newReceiverForSession(ctx, session, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeSecond.Ptr(),
	}, receiverTestHooks{MuxSelect: muxSem.OnLoop})
	cancel()
	require.NoError(t, err)
	msgChan := make(chan *Message)
	errChan := make(chan error)
	go func() {
		msg, err := r.Receive(context.Background(), nil)
		msgChan <- msg
		errChan <- err
	}()
	// send multi-frame message
	payload := []byte("this should be split into three frames for a multi-frame transfer message")
	require.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, nil))
	msg := <-msgChan
	require.NoError(t, <-errChan)
	// validate message content
	result := []byte{}
	for i := range msg.Data {
		result = append(result, msg.Data[i]...)
	}
	require.Equal(t, payload, result)
	muxSem.Wait()
	if c := r.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// link credit must be zero since we only started with 1
	if c := r.l.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(2)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	require.NoError(t, err)
	muxSem.Wait()
	if c := r.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	require.Equal(t, true, msg.settled)
	// link credit should be back to 1
	if c := r.l.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(-1)
	require.NoError(t, client.Close())
}

func TestReceiveInvalidMultiFrameMessage(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		switch tt := req.(type) {
		case *fake.AMQPProto:
			return newResponse(fake.ProtoHeader(fake.ProtoAMQP))
		case *frames.PerformOpen:
			return newResponse(fake.PerformOpen("container"))
		case *frames.PerformClose:
			return newResponse(fake.PerformClose(nil))
		case *frames.PerformBegin:
			return newResponse(fake.PerformBegin(0, remoteChannel))
		case *frames.PerformEnd:
			return newResponse(fake.PerformEnd(0, nil))
		case *frames.PerformAttach:
			return newResponse(fake.ReceiverAttach(0, tt.Name, 0, ReceiverSettleModeSecond, tt.Source.Filter))
		case *frames.PerformDetach:
			return newResponse(fake.PerformDetach(0, 0, nil))
		case *frames.PerformFlow, *fake.KeepAlive:
			return fake.Response{}, nil
		case *frames.PerformDisposition:
			return newResponse(fake.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{}))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	msgChan := make(chan *Message)
	errChan := make(chan error)
	go func() {
		msg, err := r.Receive(context.Background(), nil)
		msgChan <- msg
		errChan <- err
	}()
	// send multi-frame message
	payload := []byte("this should be split into two frames for a multi-frame transfer")

	// mismatched DeliveryID
	require.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, func(i int, fr *frames.PerformTransfer) {
		if i == 0 {
			return
		}
		// modify the second frame with mismatched data
		badID := uint32(123)
		fr.DeliveryID = &badID
	}))
	msg := <-msgChan
	require.Nil(t, msg)
	var linkErr *LinkError
	require.ErrorAs(t, <-errChan, &linkErr)
	require.Contains(t, linkErr.Error(), ErrCondNotAllowed)

	// mismatched MessageFormat
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err = session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	go func() {
		msg, err := r.Receive(context.Background(), nil)
		msgChan <- msg
		errChan <- err
	}()
	require.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, func(i int, fr *frames.PerformTransfer) {
		if i == 0 {
			return
		}
		// modify the second frame with mismatched data
		badFormat := uint32(123)
		fr.MessageFormat = &badFormat
	}))
	msg = <-msgChan
	require.Nil(t, msg)
	require.ErrorAs(t, <-errChan, &linkErr)
	require.Contains(t, linkErr.Error(), ErrCondNotAllowed)

	// mismatched DeliveryTag
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err = session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	go func() {
		msg, err := r.Receive(context.Background(), nil)
		msgChan <- msg
		errChan <- err
	}()
	require.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, func(i int, fr *frames.PerformTransfer) {
		if i == 0 {
			return
		}
		// modify the second frame with mismatched data
		fr.DeliveryTag = []byte("bad_tag")
	}))
	msg = <-msgChan
	require.Nil(t, msg)
	require.ErrorAs(t, <-errChan, &linkErr)
	require.Contains(t, linkErr.Error(), ErrCondNotAllowed)

	require.NoError(t, client.Close())
}

func TestReceiveMultiFrameMessageAborted(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		resp, err := receiverFrameHandler(0, ReceiverSettleModeSecond)(remoteChannel, req)
		if resp.Payload != nil || err != nil {
			return resp, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow, *fake.KeepAlive:
			return fake.Response{}, nil
		case *frames.PerformDisposition:
			if _, ok := ff.State.(*encoding.StateAccepted); !ok {
				return fake.Response{}, fmt.Errorf("unexpected State %T", ff.State)
			}
			return newResponse(fake.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{}))
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeSecond.Ptr(),
	})
	cancel()
	require.NoError(t, err)
	msgChan := make(chan *Message)
	errChan := make(chan error)
	go func() {
		msg, err := r.Receive(context.Background(), nil)
		errChan <- err
		msgChan <- msg
	}()
	// send multi-frame message
	payload := []byte("this should be split into three frames for a multi-frame transfer message")
	require.NoError(t, conn.SendMultiFrameTransfer(0, linkHandle, deliveryID, payload, func(i int, fr *frames.PerformTransfer) {
		if i < 2 {
			return
		}
		// set abort flag on the last frame
		fr.Aborted = true
	}))
	// we shouldn't have received any message at this point, now send a single-frame message
	payload = []byte("single message")
	b, err := fake.PerformTransfer(0, linkHandle, deliveryID+1, payload)
	require.NoError(t, err)
	conn.SendFrame(b)
	require.NoError(t, <-errChan)
	msg := <-msgChan
	require.Equal(t, payload, msg.GetData())
	require.NoError(t, client.Close())
}

func TestReceiveMessageTooBig(t *testing.T) {
	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		resp, err := receiverFrameHandler(0, ReceiverSettleModeSecond)(remoteChannel, req)
		if resp.Payload != nil || err != nil {
			return resp, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				bigPayload := make([]byte, 256)
				return newResponse(fake.PerformTransfer(0, linkHandle, deliveryID, bigPayload))
			}
			// ignore future flow frames as we have no response
			return fake.Response{}, nil
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeSecond.Ptr(),
		MaxMessageSize: 128,
	})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx, nil)
	cancel()
	require.Nil(t, msg)
	var linkErr *LinkError
	require.ErrorAs(t, err, &linkErr)
	require.Contains(t, linkErr.Error(), ErrCondMessageSizeExceeded)
	require.NoError(t, client.Close())
}

func TestReceiveSuccessAcceptFails(t *testing.T) {
	muxSem := test.NewMuxSemaphore(2)

	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		resp, err := receiverFrameHandler(0, ReceiverSettleModeSecond)(remoteChannel, req)
		if resp.Payload != nil || err != nil {
			return resp, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return newResponse(fake.PerformTransfer(0, linkHandle, deliveryID, []byte("hello")))
			}
			// ignore future flow frames as we have no response
			return fake.Response{}, nil
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := newReceiverForSession(ctx, session, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeSecond.Ptr(),
	}, receiverTestHooks{MuxSelect: muxSem.OnLoop})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx, nil)
	cancel()
	require.NoError(t, err)
	muxSem.Wait()
	if c := r.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	// link credit must be zero since we only started with 1
	if c := r.l.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(-1)
	// close client before accepting the message
	require.NoError(t, client.Close())
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	var connErr *ConnError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	if c := r.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
}

func TestReceiverCloseOnUnsettledWithPending(t *testing.T) {
	conn := fake.NewNetConn(receiverFrameHandlerNoUnhandled(0, ReceiverSettleModeFirst))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	// first message exhausts the link credit
	b, err := fake.PerformTransfer(0, 0, 1, []byte("message 1"))
	require.NoError(t, err)
	conn.SendFrame(b)

	// wait for the messages to "arrive"
	time.Sleep(time.Second)

	// now close the receiver without reading any of the messages
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, r.Close(ctx))
	cancel()
}

func TestReceiverConnReaderError(t *testing.T) {
	conn := fake.NewNetConn(receiverFrameHandlerNoUnhandled(0, ReceiverSettleModeFirst))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background(), nil)
		errChan <- err
	}()

	// trigger some kind of error
	conn.ReadErr <- errors.New("failed")

	err = <-errChan
	var connErr *ConnError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	_, err = r.Receive(context.Background(), nil)
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	require.Error(t, conn.Close())
}

func TestReceiverConnWriterError(t *testing.T) {
	conn := fake.NewNetConn(receiverFrameHandlerNoUnhandled(0, ReceiverSettleModeFirst))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := session.NewReceiver(ctx, "source", nil)
	cancel()
	require.NoError(t, err)

	errChan := make(chan error)
	go func() {
		_, err := r.Receive(context.Background(), nil)
		errChan <- err
	}()

	conn.WriteErr <- errors.New("failed")
	// trigger the write error
	conn.SendKeepAlive()

	err = <-errChan
	var connErr *ConnError
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	_, err = r.Receive(context.Background(), nil)
	if !errors.As(err, &connErr) {
		t.Fatalf("unexpected error type %T", err)
	}
	require.Error(t, conn.Close())
}

func TestReceiveSuccessReceiverSettleModeSecondAcceptSlow(t *testing.T) {
	muxSem := test.NewMuxSemaphore(2)

	const linkHandle = 0
	deliveryID := uint32(1)
	responder := func(remoteChannel uint16, req frames.FrameBody) (fake.Response, error) {
		resp, err := receiverFrameHandler(0, ReceiverSettleModeSecond)(remoteChannel, req)
		if resp.Payload != nil || err != nil {
			return resp, err
		}
		switch ff := req.(type) {
		case *frames.PerformFlow:
			if *ff.NextIncomingID == deliveryID {
				// this is the first flow frame, send our payload
				return newResponse(fake.PerformTransfer(0, linkHandle, deliveryID, []byte("hello")))
			}
			// ignore future flow frames as we have no response
			return fake.Response{}, nil
		case *frames.PerformDisposition:
			b, err := fake.PerformDisposition(encoding.RoleSender, 0, deliveryID, nil, &encoding.StateAccepted{})
			if err != nil {
				return fake.Response{}, err
			}
			// include a write delay so that waiting for the ack times out
			return fake.Response{Payload: b, WriteDelay: 1 * time.Second}, nil
		default:
			return fake.Response{}, fmt.Errorf("unhandled frame %T", req)
		}
	}
	conn := fake.NewNetConn(responder)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	client, err := NewConn(ctx, conn, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	session, err := client.NewSession(ctx, nil)
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
	r, err := newReceiverForSession(ctx, session, "source", &ReceiverOptions{
		SettlementMode: ReceiverSettleModeSecond.Ptr(),
	}, receiverTestHooks{MuxSelect: muxSem.OnLoop})
	cancel()
	require.NoError(t, err)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	msg, err := r.Receive(ctx, nil)
	cancel()
	require.NoError(t, err)
	if c := r.countUnsettled(); c != 1 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	muxSem.Wait()
	// link credit must be zero since we only started with 1
	if c := r.l.linkCredit; c != 0 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(2)
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = r.AcceptMessage(ctx, msg)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	muxSem.Wait()
	// even though we timed out waiting for the ack, the message should still be settled
	if c := r.countUnsettled(); c != 0 {
		t.Fatalf("unexpected unsettled count %d", c)
	}
	require.True(t, msg.settled)
	// link credit should be back to 1
	if c := r.l.linkCredit; c != 1 {
		t.Fatalf("unexpected link credit %d", c)
	}
	muxSem.Release(-1)
	require.NoError(t, client.Close())
}

// TODO: add unit tests for manual credit management
