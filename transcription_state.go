package main

import (
	"context"
	"sync"
)

// transcriptionState follows committed input items until each transcription
// has completed or failed. Completion events may arrive out of order.
type transcriptionState struct {
	mu             sync.Mutex
	pending        map[string]struct{}
	finished       map[string]struct{}
	commits        int
	sessionUpdates int
	commitFailed   bool
	changed        chan struct{}
}

func newTranscriptionState() *transcriptionState {
	return &transcriptionState{
		pending:  make(map[string]struct{}),
		finished: make(map[string]struct{}),
		changed:  make(chan struct{}),
	}
}

func (s *transcriptionState) signal() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func (s *transcriptionState) committed(itemID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commits++
	if _, done := s.finished[itemID]; done {
		delete(s.finished, itemID)
	} else {
		s.pending[itemID] = struct{}{}
	}
	s.signal()
}

func (s *transcriptionState) terminal(itemID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, committed := s.pending[itemID]; committed {
		delete(s.pending, itemID)
	} else {
		s.finished[itemID] = struct{}{}
	}
	s.signal()
}

func (s *transcriptionState) sessionUpdated() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessionUpdates++
	s.signal()
}

func (s *transcriptionState) stopCommitFailed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitFailed = true
	s.signal()
}

func (s *transcriptionState) snapshot() (commits, sessionUpdates int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commits, s.sessionUpdates
}

func (s *transcriptionState) wait(ctx context.Context, commitsBefore, updatesBefore int, requireCommitResult, requireBarrier bool) error {
	for {
		s.mu.Lock()
		barrierReady := !requireBarrier || s.sessionUpdates > updatesBefore
		commitResolved := !requireCommitResult || s.commits > commitsBefore || s.commitFailed
		if barrierReady && commitResolved && len(s.pending) == 0 {
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()

		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
