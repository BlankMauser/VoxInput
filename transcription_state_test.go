package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestTranscriptionStateWaitsForFinalVADItem(t *testing.T) {
	state := newTranscriptionState()
	state.committed("first")
	state.terminal("first")
	commitsBefore, updatesBefore := state.snapshot()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := state.wait(ctx, commitsBefore, updatesBefore, true, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait after earlier transcript = %v, want canceled", err)
	}

	state.committed("last")
	state.sessionUpdated()
	if err := state.wait(ctx, commitsBefore, updatesBefore, true, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait while final item is pending = %v, want canceled", err)
	}
	state.terminal("last")
	if err := state.wait(context.Background(), commitsBefore, updatesBefore, true, true); err != nil {
		t.Fatalf("wait after final transcript: %v", err)
	}
}

func TestTranscriptionStateFailedCommitStillWaitsForPriorItems(t *testing.T) {
	state := newTranscriptionState()
	state.committed("vad-item")
	commitsBefore, updatesBefore := state.snapshot()
	state.stopCommitFailed()
	state.sessionUpdated()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := state.wait(ctx, commitsBefore, updatesBefore, true, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait while VAD item is pending = %v, want canceled", err)
	}
	state.terminal("vad-item")
	if err := state.wait(context.Background(), commitsBefore, updatesBefore, true, true); err != nil {
		t.Fatalf("wait after VAD item completed: %v", err)
	}
}

func TestTranscriptionStateAcceptsTerminalBeforeCommitEvent(t *testing.T) {
	state := newTranscriptionState()
	commitsBefore, updatesBefore := state.snapshot()
	state.terminal("item")
	state.committed("item")
	state.sessionUpdated()
	if err := state.wait(context.Background(), commitsBefore, updatesBefore, true, true); err != nil {
		t.Fatalf("wait after out-of-order events: %v", err)
	}
	if len(state.pending) != 0 || len(state.finished) != 0 {
		t.Fatalf("matched item retained: pending=%v finished=%v", state.pending, state.finished)
	}
}

func TestTranscriptionStateReleasesMatchedItemsDuringLongSession(t *testing.T) {
	state := newTranscriptionState()
	for i := range 1000 {
		itemID := fmt.Sprintf("item-%d", i)
		if i%2 == 0 {
			state.committed(itemID)
			state.terminal(itemID)
		} else {
			state.terminal(itemID)
			state.committed(itemID)
		}
		if len(state.pending) != 0 || len(state.finished) != 0 {
			t.Fatalf("item %s retained: pending=%v finished=%v", itemID, state.pending, state.finished)
		}
	}
}
