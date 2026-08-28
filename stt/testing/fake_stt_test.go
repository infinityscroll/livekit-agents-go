// SPDX-License-Identifier: Apache-2.0

package stttest

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/stt"
)

func stringPointer(value string) *string { return &value }
func boolPointer(value bool) *bool       { return &value }

func TestFakeSTTBatchUpdateAndObservability(t *testing.T) {
	fake, err := NewFakeSTT(FakeSTTOptions{FakeTranscript: stringPointer("first"), ObservationCapacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer fake.Close(context.Background())

	event, err := fake.Recognize(context.Background(), []agents.AudioFrame{EmptyAudioFrame()}, stt.RecognizeOptions{})
	if err != nil || event.Alternatives[0].Text != "first" {
		t.Fatalf("event=%+v err=%v", event, err)
	}
	if _, err := fake.RecognizeCalls().Recv(context.Background()); err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	if err := fake.UpdateOptions(FakeSTTUpdateOptions{FakeException: agents.Use[error](boom), FakeTranscript: agents.Use("second")}); err != nil {
		t.Fatal(err)
	}
	_, err = fake.Recognize(context.Background(), nil, stt.RecognizeOptions{})
	if !errors.Is(err, boom) {
		t.Fatalf("expected injected error, got %v", err)
	}
	if err := fake.UpdateOptions(FakeSTTUpdateOptions{FakeException: agents.Disable[error]()}); err != nil {
		t.Fatal(err)
	}
	event, err = fake.Recognize(context.Background(), nil, stt.RecognizeOptions{})
	if err != nil || event.Alternatives[0].Text != "second" {
		t.Fatalf("event=%+v err=%v", event, err)
	}
}

func TestFakeSTTScheduledInterimFinalAndClose(t *testing.T) {
	fake, err := NewFakeSTT(FakeSTTOptions{FakeUserSpeeches: []FakeUserSpeech{{
		StartTime: time.Millisecond, EndTime: 4 * time.Millisecond, STTDelay: 4 * time.Millisecond,
		Transcript: "hello brave world",
	}}, StreamCapacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer fake.Close(context.Background())
	value, err := fake.Stream(context.Background(), stt.StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	created, err := fake.Streams().Recv(context.Background())
	if err != nil || created != value {
		t.Fatalf("created=%p value=%p err=%v", created, value, err)
	}
	frame, _ := agents.NewAudioFrame(make([]int16, 160), 16_000, 1)
	if err := value.Push(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	if err := value.EndInput(); err != nil {
		t.Fatal(err)
	}
	interim, err := value.Recv(context.Background())
	if err != nil || interim.Type != stt.InterimTranscript || interim.Alternatives[0].Text != "hello brave" {
		t.Fatalf("interim=%+v err=%v", interim, err)
	}
	final, err := value.Recv(context.Background())
	if err != nil || final.Type != stt.FinalTranscript || final.Alternatives[0].Text != "hello brave world" {
		t.Fatalf("final=%+v err=%v", final, err)
	}
	if _, err := value.Recv(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
	select {
	case <-fake.FakeUserSpeechesDone():
	case <-time.After(time.Second):
		t.Fatal("scheduled speech did not finish")
	}
}

func TestFakeSTTRequireAudioAndCancellation(t *testing.T) {
	fake, err := NewFakeSTT(FakeSTTOptions{FakeTranscript: stringPointer("heard"), FakeRequireAudio: true})
	if err != nil {
		t.Fatal(err)
	}
	defer fake.Close(context.Background())
	value, err := fake.Stream(context.Background(), stt.StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := value.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	frame, _ := agents.NewAudioFrame(make([]int16, 16), 16_000, 1)
	if err := value.Push(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	if err := value.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	event, err := value.Recv(context.Background())
	if err != nil || event.Alternatives[0].Text != "heard" {
		t.Fatalf("event=%+v err=%v", event, err)
	}
	_ = value.Close()

	timed, err := NewFakeSTT(FakeSTTOptions{FakeTimeout: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = timed.Recognize(ctx, nil, stt.RecognizeOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	_ = timed.Close(context.Background())
}

func TestFakeSTTRejectsOverlapAndNonFinalSpeech(t *testing.T) {
	_, err := NewFakeSTT(FakeSTTOptions{FakeUserSpeeches: []FakeUserSpeech{
		{StartTime: 0, EndTime: time.Second},
		{StartTime: time.Millisecond, EndTime: 2 * time.Second},
	}})
	if err == nil {
		t.Fatal("expected overlap error")
	}
	fake, err := NewFakeSTT(FakeSTTOptions{FakeUserSpeeches: []FakeUserSpeech{{Transcript: "partial only", Final: boolPointer(false)}}})
	if err != nil {
		t.Fatal(err)
	}
	value, _ := fake.Stream(context.Background(), stt.StreamOptions{})
	_ = value.Push(context.Background(), EmptyAudioFrame())
	_ = value.EndInput()
	event, err := value.Recv(context.Background())
	if err != nil || event.Type != stt.InterimTranscript {
		t.Fatalf("event=%+v err=%v", event, err)
	}
	if _, err := value.Recv(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
	_ = fake.Close(context.Background())
}
