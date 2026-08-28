// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidAudioFormat = errors.New("invalid audio format")
	ErrIncompletePCMFrame = errors.New("incomplete PCM16 frame")
)

// AudioFrame is interleaved signed 16-bit PCM. Data is owned by the frame and
// remains valid until the frame is discarded.
type AudioFrame struct {
	Data              []int16
	SampleRate        int
	Channels          int
	SamplesPerChannel int
	UserData          map[string]any
}

func NewAudioFrame(data []int16, sampleRate, channels int) (AudioFrame, error) {
	if sampleRate <= 0 || channels <= 0 || len(data)%channels != 0 {
		return AudioFrame{}, fmt.Errorf("%w: rate=%d channels=%d samples=%d", ErrInvalidAudioFormat, sampleRate, channels, len(data))
	}
	return AudioFrame{Data: data, SampleRate: sampleRate, Channels: channels, SamplesPerChannel: len(data) / channels}, nil
}

func (f AudioFrame) Duration() time.Duration {
	if f.SampleRate <= 0 {
		return 0
	}
	return time.Duration(f.SamplesPerChannel) * time.Second / time.Duration(f.SampleRate)
}

func CalculateAudioDuration(frames []AudioFrame) time.Duration {
	var d time.Duration
	for i := range frames {
		d += frames[i].Duration()
	}
	return d
}

// MergeFrames combines same-format frames with one allocation.
func MergeFrames(frames []AudioFrame) (AudioFrame, error) {
	if len(frames) == 0 {
		return AudioFrame{}, fmt.Errorf("%w: no frames", ErrInvalidAudioFormat)
	}
	rate, channels := frames[0].SampleRate, frames[0].Channels
	total := 0
	for i := range frames {
		if frames[i].SampleRate != rate || frames[i].Channels != channels {
			return AudioFrame{}, fmt.Errorf("%w: frame %d format differs", ErrInvalidAudioFormat, i)
		}
		total += len(frames[i].Data)
	}
	data := make([]int16, total)
	off := 0
	for i := range frames {
		off += copy(data[off:], frames[i].Data)
	}
	return NewAudioFrame(data, rate, channels)
}

// AudioByteStream packetizes little-endian PCM16 without repeatedly copying
// the accumulated input. It is not safe for concurrent use.
type AudioByteStream struct {
	sampleRate    int
	channels      int
	bytesPerFrame int
	pending       []byte
}

func NewAudioByteStream(sampleRate, channels, samplesPerChannel int) (*AudioByteStream, error) {
	if sampleRate <= 0 || channels <= 0 {
		return nil, fmt.Errorf("%w: rate=%d channels=%d", ErrInvalidAudioFormat, sampleRate, channels)
	}
	if samplesPerChannel <= 0 {
		samplesPerChannel = sampleRate / 10
	}
	return &AudioByteStream{
		sampleRate:    sampleRate,
		channels:      channels,
		bytesPerFrame: channels * samplesPerChannel * 2,
		pending:       make([]byte, 0, channels*samplesPerChannel*2),
	}, nil
}

func (s *AudioByteStream) Write(data []byte) []AudioFrame {
	if len(data) == 0 {
		return nil
	}
	estimated := (len(s.pending) + len(data)) / s.bytesPerFrame
	frames := make([]AudioFrame, 0, estimated)

	if len(s.pending) != 0 {
		need := s.bytesPerFrame - len(s.pending)
		if need > len(data) {
			s.pending = append(s.pending, data...)
			return frames
		}
		s.pending = append(s.pending, data[:need]...)
		frames = append(frames, s.decode(s.pending))
		s.pending = s.pending[:0]
		data = data[need:]
	}

	for len(data) >= s.bytesPerFrame {
		frames = append(frames, s.decode(data[:s.bytesPerFrame]))
		data = data[s.bytesPerFrame:]
	}
	if len(data) != 0 {
		s.pending = append(s.pending, data...)
	}
	return frames
}

func (s *AudioByteStream) Flush() ([]AudioFrame, error) {
	if len(s.pending) == 0 {
		return nil, nil
	}
	if len(s.pending)%(2*s.channels) != 0 {
		s.pending = s.pending[:0]
		return nil, ErrIncompletePCMFrame
	}
	frame := s.decode(s.pending)
	s.pending = s.pending[:0]
	return []AudioFrame{frame}, nil
}

func (s *AudioByteStream) Reset() { s.pending = s.pending[:0] }

func (s *AudioByteStream) decode(b []byte) AudioFrame {
	data := make([]int16, len(b)/2)
	for i := range data {
		data[i] = int16(binary.LittleEndian.Uint16(b[i*2:]))
	}
	frame, _ := NewAudioFrame(data, s.sampleRate, s.channels)
	return frame
}
