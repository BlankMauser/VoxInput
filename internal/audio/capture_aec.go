package audio

import (
	"context"
	"fmt"
	"io"

	"github.com/gen2brain/malgo"
)

// CaptureWithAEC records microphone audio while using an independent monitor
// capture as the far-end reference. Inference runs off the audio callback.
func CaptureWithAEC(
	ctx context.Context,
	w io.Writer,
	config StreamConfig,
	processor AudioProcessor,
	monitor *Int16Ring,
) error {
	if config.Format != malgo.FormatS16 || config.Channels != 1 {
		return fmt.Errorf("AEC capture requires mono signed 16-bit PCM")
	}
	if config.SampleRate < 1 || (config.InputSampleRate != 0 && config.InputSampleRate != config.SampleRate) {
		return fmt.Errorf("AEC capture requires matching positive device and input sample rates")
	}

	periodMs := config.PeriodMs
	if periodMs == 0 {
		periodMs = 20
	}
	periodSamples := max(1, periodMs*config.SampleRate/1000)
	micRing := NewInt16Ring(config.SampleRate)
	refRing := NewInt16Ring(config.SampleRate)
	micScratch := make([]int16, 2*periodSamples)
	refScratch := make([]int16, 2*periodSamples)

	// The monitor is captured while idle. Keep only its newest period so the
	// first microphone callback does not pair with stale system audio.
	monitor.TrimTo(periodSamples)

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	worker := NewAECWorker(
		workerCtx, processor, micRing, refRing,
		config.SampleRate, config.InputSampleRate, w, nil,
	)
	defer func() {
		cancelWorker()
		<-worker.Done()
	}()

	deviceCallbacks := malgo.DeviceCallbacks{
		Data: func(_, inputSamples []byte, _ uint32) {
			n := len(inputSamples) / 2
			if n == 0 {
				return
			}
			if n > cap(micScratch) {
				micScratch = make([]int16, n)
				refScratch = make([]int16, n)
			}
			mic := micScratch[:n]
			ref := refScratch[:n]
			bytesToS16Into(mic, inputSamples[:n*2])
			// Independent device clocks can drift. Bound the reference queue
			// to two callbacks so an old monitor backlog never trails the mic.
			monitor.TrimTo(2 * n)
			monitor.Read(ref)
			micRing.Write(mic)
			refRing.Write(ref)
		},
	}

	return stream(ctx, make(chan error), config, malgo.Capture, deviceCallbacks)
}
