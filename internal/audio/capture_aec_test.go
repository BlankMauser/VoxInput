package audio

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
)

type passthroughProcessor struct{}

func (passthroughProcessor) Process(rec, _ []byte, out []byte) int {
	return copy(out, rec)
}

func TestAECWorkerFlushesPartialFinalFrame(t *testing.T) {
	mic := NewInt16Ring(24000)
	ref := NewInt16Ring(24000)
	in := make([]int16, 100)
	for i := range in {
		in[i] = int16(i + 1)
	}
	mic.Write(in)
	ref.Write(make([]int16, len(in)))

	var out bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	worker := NewAECWorker(ctx, passthroughProcessor{}, mic, ref, 24000, 24000, &out, nil)
	cancel()
	<-worker.Done()

	if got, want := out.Len(), 24000/50*2; got != want {
		t.Fatalf("final frame bytes = %d, want %d", got, want)
	}
	for i, want := range in {
		if got := int16(binary.LittleEndian.Uint16(out.Bytes()[i*2:])); got != want {
			t.Fatalf("sample %d = %d, want %d", i, got, want)
		}
	}
}

func TestMonitorRingTrimsStaleAudio(t *testing.T) {
	ring := NewInt16Ring(10)
	ring.Write([]int16{1, 2, 3, 4, 5, 6})
	ring.TrimTo(2)
	got := make([]int16, 2)
	if n := ring.Read(got); n != 2 || got[0] != 5 || got[1] != 6 {
		t.Fatalf("latest samples = %v (read %d), want [5 6]", got, n)
	}
}
