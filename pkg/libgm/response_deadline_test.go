package libgm

import (
	"errors"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

// A request that never receives an answer must fail instead of blocking the
// caller forever. Before the response deadline existed, waitForResponse only
// nudged the pinger after 5s and then blocked on the channel indefinitely
// (the "TODO hard timeout?" branch), which hung a deep backfill outright.
func TestResponseDeadlineFailsHungRequest(t *testing.T) {
	cli, logs := newTestClient(t)
	cli.SetResponseDeadline(10 * time.Millisecond)

	ch := await(cli, "req-hung", gmproto.ActionType_LIST_MESSAGES)

	var r result
	select {
	case r = <-ch:
	case <-time.After(8 * time.Second):
		t.Fatal("request never completed: the deadline did not fire")
	}

	var timeoutErr *ResponseTimeoutError
	if !errors.As(r.err, &timeoutErr) {
		t.Fatalf("expected ResponseTimeoutError, got resp=%v err=%v", r.resp != nil, r.err)
	}
	if timeoutErr.RequestID != "req-hung" {
		t.Fatalf("expected the request id, got %q", timeoutErr.RequestID)
	}
	if timeoutErr.Waited < 5*time.Second {
		t.Fatalf("expected the wait to include the 5s ping nudge, got %s", timeoutErr.Waited)
	}

	// The waiter must be unregistered, so a late frame is ignored rather than
	// filling a channel nobody reads.
	cli.sessionHandler.responseWaitersLock.Lock()
	_, pending := cli.sessionHandler.responseWaiters["req-hung"]
	cli.sessionHandler.responseWaitersLock.Unlock()
	if pending {
		t.Fatal("waiter leaked after the deadline fired")
	}
	if !strings.Contains(logs.String(), "response deadline") {
		t.Fatalf("expected a deadline warning in the log, got %q", logs.String())
	}
}

// A late answer after the deadline must not resurrect the request or panic.
func TestResponseDeadlineIgnoresLateFrame(t *testing.T) {
	cli, _ := newTestClient(t)
	cli.SetResponseDeadline(10 * time.Millisecond)

	ch := await(cli, "req-late", gmproto.ActionType_LIST_MESSAGES)
	select {
	case r := <-ch:
		var timeoutErr *ResponseTimeoutError
		if !errors.As(r.err, &timeoutErr) {
			t.Fatalf("expected ResponseTimeoutError, got %v", r.err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("request never completed")
	}

	msg := cli.encodeFrame(t, frame{
		requestID: "req-late",
		action:    gmproto.ActionType_LIST_MESSAGES,
		payload:   &gmproto.ListMessagesResponse{},
	})
	// Must be dropped, not delivered: the waiter is gone, so this only logs.
	cli.HandleRPCMsg(msg)

	cli.sessionHandler.responseWaitersLock.Lock()
	_, pending := cli.sessionHandler.responseWaiters["req-late"]
	cli.sessionHandler.responseWaitersLock.Unlock()
	if pending {
		t.Fatal("a late frame resurrected a waiter that had already timed out")
	}
}
