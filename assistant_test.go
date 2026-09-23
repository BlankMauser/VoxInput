package main

import (
	"bytes"
	"context"
	"testing"

	openairt "github.com/WqyJh/go-openai-realtime/v2"
	"github.com/richiejp/VoxInput/internal/audio"
)

func TestAssistantAudioLedgerTruncatesAtPlaybackBoundary(t *testing.T) {
	chunks := make(chan *bytes.Buffer, 2)
	reader := audio.NewChunkReader(context.Background(), chunks, 0)
	var ledger assistantAudioLedger
	first := openairt.ResponseOutputAudioDeltaEvent{
		ResponseID:   "response-1",
		ItemID:       "item-1",
		ContentIndex: 0,
	}
	part := ledger.noteDelta(first, 48000)
	chunks <- bytes.NewBuffer(make([]byte, 48000))
	ledger.noteQueued(part, 48000)

	read, err := reader.Read(make([]byte, 24000))
	if err != nil || read != 24000 {
		t.Fatalf("playback read = %d, %v", read, err)
	}
	dropped, progress := reader.FlushWithProgress()
	if dropped != 24000 {
		t.Fatalf("dropped = %d, want 24000", dropped)
	}
	// The server may have finished generating while half its audio remained
	// queued locally. That item still needs to be truncated.
	events := ledger.truncations(progress, "", 24000)
	if len(events) != 1 || events[0].ItemID != "item-1" || events[0].AudioEndMs != 500 {
		t.Fatalf("truncations = %+v, want item-1 at 500 ms", events)
	}

	ledger.parts = nil
	second := openairt.ResponseOutputAudioDeltaEvent{
		ResponseID:   "response-2",
		ItemID:       "item-2",
		ContentIndex: 1,
	}
	part = ledger.noteDelta(second, 24000)
	chunks <- bytes.NewBuffer(make([]byte, 24000))
	ledger.noteQueued(part, 24000)
	read, err = reader.Read(make([]byte, 12000))
	if err != nil || read != 12000 {
		t.Fatalf("second playback read = %d, %v", read, err)
	}
	_, progress = reader.FlushWithProgress()
	events = ledger.truncations(progress, "response-2", 24000)
	if len(events) != 1 || events[0].ItemID != "item-2" || events[0].ContentIndex != 1 || events[0].AudioEndMs != 250 {
		t.Fatalf("second truncations = %+v, want item-2 content 1 at 250 ms", events)
	}
}

func TestAssistantAudioLedgerSkipsFullyPlayedResponse(t *testing.T) {
	var ledger assistantAudioLedger
	delta := openairt.ResponseOutputAudioDeltaEvent{
		ResponseID: "response-1",
		ItemID:     "item-1",
	}
	part := ledger.noteDelta(delta, 48000)
	ledger.noteQueued(part, 48000)
	events := ledger.truncations(audio.PlaybackProgress{PlayedBytes: 48000}, "", 24000)
	if len(events) != 0 {
		t.Fatalf("fully played response should not be truncated: %+v", events)
	}
}
