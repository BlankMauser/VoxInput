package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	openairt "github.com/WqyJh/go-openai-realtime/v2"
	"github.com/richiejp/VoxInput/internal/audio"
	"github.com/richiejp/VoxInput/internal/gui"
	"github.com/richiejp/VoxInput/internal/ipc"
)

const stopTranscriptionCommitEventID = "voxinput-stop-transcription-commit"

func (l *Listener) startTranscriptionSession(ctx context.Context) error {
	return l.conn.SendMessage(ctx, l.transcriptionSessionUpdate("Initial update"))
}

func (l *Listener) transcriptionSessionUpdate(eventID string) openairt.SessionUpdateEvent {
	var transcription *openairt.AudioTranscription
	if l.config.Model != "" {
		transcription = &openairt.AudioTranscription{
			Model:    l.config.Model,
			Language: l.config.Lang,
			Prompt:   l.config.Prompt,
		}
	}

	return openairt.SessionUpdateEvent{
		EventBase: openairt.EventBase{
			EventID: eventID,
		},
		Session: openairt.SessionUnion{
			Transcription: &openairt.TranscriptionSession{
				Audio: &openairt.TranscriptionSessionAudio{
					Input: &openairt.SessionAudioInput{
						Transcription: transcription,
						TurnDetection: &openairt.TurnDetectionUnion{
							ServerVad: &openairt.ServerVad{},
						},
					},
				},
			},
		},
	}
}

func (l *Listener) runAudioTranscription() {
	var err error
	if l.processor != nil && l.config.RefRing != nil {
		err = audio.CaptureWithAEC(
			l.captureCtx, l.chunkWriter, l.streamConfig,
			l.processor, l.config.RefRing,
		)
	} else {
		err = audio.Capture(l.captureCtx, l.chunkWriter, l.streamConfig)
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		l.errCh <- fmt.Errorf("audio capture: %w", err)
		l.cancel()
	}
}

func (l *Listener) ReceiveTranscriptionMessages() {
	for {
		msg, err := l.conn.ReadMessage(l.ctx)
		if err != nil {
			var permanent *openairt.PermanentError
			if errors.As(err, &permanent) {
				log.Println("Listener.ReceiveTranscriptionMessages: Connection failed: ", err)
				l.cancel()
				return
			}
			log.Println("Listener.ReceiveTranscriptionMessages: error receiving message, retrying: ", err)
			continue
		}
		log.Println("Listener.ReceiveTranscriptionMessages: receiving message: ", msg.ServerEventType())
		var text string
		var terminalItemID string
		switch msg.ServerEventType() {
		case openairt.ServerEventTypeInputAudioBufferSpeechStarted:
			log.Println("Listener.ReceiveTranscriptionMessages: speech detected")
			l.config.UI.Send(&gui.ShowSpeechDetectedMsg{})
			if l.config.IPCServer != nil {
				l.config.IPCServer.Broadcast(ipc.Event{
					Kind: ipc.EventSpeechStarted,
					Ts:   time.Now().UnixMilli(),
				})
			}
		case openairt.ServerEventTypeInputAudioBufferSpeechStopped:
			log.Println("Listener.ReceiveTranscriptionMessages: speech stopped, transcribing")
			l.config.UI.Send(&gui.ShowTranscribingMsg{})
			if l.config.IPCServer != nil {
				l.config.IPCServer.Broadcast(ipc.Event{
					Kind: ipc.EventSpeechStopped,
					Ts:   time.Now().UnixMilli(),
				})
			}
		case openairt.ServerEventTypeResponseOutputAudioTranscriptDone:
			text = msg.(openairt.ResponseOutputAudioTranscriptDoneEvent).Transcript
		case openairt.ServerEventTypeInputAudioBufferCommitted:
			l.transcription.committed(msg.(openairt.InputAudioBufferCommittedEvent).ItemID)
			continue
		case openairt.ServerEventTypeSessionUpdated:
			l.transcription.sessionUpdated()
			continue
		case openairt.ServerEventTypeConversationItemInputAudioTranscriptionCompleted:
			event := msg.(openairt.ConversationItemInputAudioTranscriptionCompletedEvent)
			text = event.Transcript
			terminalItemID = event.ItemID
		case openairt.ServerEventTypeConversationItemInputAudioTranscriptionFailed:
			event := msg.(openairt.ConversationItemInputAudioTranscriptionFailedEvent)
			log.Printf("Listener.ReceiveTranscriptionMessages: item %s transcription failed: %s", event.ItemID, event.Error.Message)
			l.transcription.terminal(event.ItemID)
			continue
		case openairt.ServerEventTypeError:
			event := msg.(openairt.ErrorEvent)
			log.Println("Listener.ReceiveTranscriptionMessages: server error: ", event.Error.Message)
			if event.Error.EventID == stopTranscriptionCommitEventID {
				l.transcription.stopCommitFailed()
			}
			continue
		default:
			continue
		}
		if text == "" {
			if terminalItemID != "" {
				l.transcription.terminal(terminalItemID)
			}
			continue
		}
		if err := l.deliverTranscript(text); err != nil {
			if terminalItemID != "" {
				l.transcription.terminal(terminalItemID)
			}
			if errors.Is(err, context.Canceled) {
				return
			}
			l.errCh <- fmt.Errorf("deliver transcript: %w", err)
			l.cancel()
			return
		}
		if terminalItemID != "" {
			l.transcription.terminal(terminalItemID)
		}
	}
}

func (l *Listener) deliverTranscript(text string) error {
	l.config.UI.Send(&gui.HideMsg{})
	l.config.UI.Send(&gui.ShowTranscriptMsg{Text: text, IsUser: true})
	log.Println("Listener.ReceiveTranscriptionMessages: received transcribed text: ", text)
	if l.config.OutputFile != "" {
		f, err := os.OpenFile(l.config.OutputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Failed to open output file %s: %v\n", l.config.OutputFile, err)
			return nil
		}
		if _, err := fmt.Fprintln(f, text); err != nil {
			log.Printf("Failed to write to output file: %v\n", err)
		}
		if err := f.Close(); err != nil {
			log.Printf("Failed to close output file: %v\n", err)
		}
		return nil
	}
	log.Printf("Listener.ReceiveTranscriptionMessages: typing text: %q", text)
	if l.config.InputController == nil {
		log.Println("Listener.ReceiveTranscriptionMessages: no input controller available, cannot type text")
		return nil
	}
	if err := l.config.InputController.TypeText(l.ctx, text); err != nil {
		return fmt.Errorf("type text: %w", err)
	}
	log.Println("Listener.ReceiveTranscriptionMessages: text typed successfully")
	return nil
}
