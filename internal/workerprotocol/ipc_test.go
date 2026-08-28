// SPDX-License-Identifier: Apache-2.0

package workerprotocol

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"
)

func TestFramedConnRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	writer := NewFramedConn(left)
	reader := NewFramedConn(right)
	want := IPCMessage{
		Type: IPCTypeStart, Job: []byte{1, 2, 3}, URL: "wss://example.test",
		RequestID: "req-1", Method: "echo", Data: json.RawMessage(`{"value":1}`),
		ParticipantAttributes: map[string]string{"role": "agent"},
	}
	writeDone := make(chan error, 1)
	go func() { writeDone <- writer.Write(context.Background(), want) }()
	got, err := reader.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if got.Type != want.Type || got.URL != want.URL || string(got.Job) != string(want.Job) || got.ParticipantAttributes["role"] != "agent" ||
		got.RequestID != want.RequestID || got.Method != want.Method || string(got.Data) != string(want.Data) {
		t.Fatalf("round trip mismatch: got %#v, want %#v", got, want)
	}
}

func TestFramedConnReadHonorsContext(t *testing.T) {
	left, right := net.Pipe()
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	reader := NewFramedConn(left)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := reader.Read(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Read error = %v, want context deadline", err)
	}
}

func TestFramedConnRejectsInvalidFrameSize(t *testing.T) {
	left, right := net.Pipe()
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	reader := NewFramedConn(left)
	done := make(chan error, 1)
	go func() {
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], MaxIPCFrameSize+1)
		_, err := right.Write(header[:])
		done <- err
	}()
	if _, err := reader.Read(context.Background()); err == nil {
		t.Fatal("Read succeeded for an oversized frame")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestFramedConnValidationAndClose(t *testing.T) {
	left, right := net.Pipe()
	writer := NewFramedConn(left)
	if err := writer.Write(context.Background(), IPCMessage{}); err == nil {
		t.Fatal("Write succeeded without a message type")
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("second Close = %v", err)
	}
	_ = right.Close()
}
