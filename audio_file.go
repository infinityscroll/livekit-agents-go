// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/infinityscroll/livekit-agents-go/stream"
)

const (
	defaultAudioFileSampleRate = 48_000
	defaultAudioFileChannels   = 1
	defaultAudioFrameDuration  = 100 * time.Millisecond
	defaultAudioStreamCapacity = 8
	maxFFmpegErrorBytes        = 64 << 10
)

type AudioDecodeOptions struct {
	SampleRate int
	Channels   int
	// NumChannels is the agents-js compatibility name for Channels.
	NumChannels    int
	Format         string
	FrameDuration  time.Duration
	StreamCapacity int
	// FFmpegPath overrides the lazily discovered ffmpeg executable. Native
	// matching-format PCM16 WAV decoding does not start or require ffmpeg.
	FFmpegPath string
}

func (o AudioDecodeOptions) resolve() (AudioDecodeOptions, error) {
	if o.Channels != 0 && o.NumChannels != 0 && o.Channels != o.NumChannels {
		return o, errors.New("channels and NumChannels cannot specify different values")
	}
	if o.Channels == 0 {
		o.Channels = o.NumChannels
	}
	if o.SampleRate == 0 {
		o.SampleRate = defaultAudioFileSampleRate
	}
	if o.Channels == 0 {
		o.Channels = defaultAudioFileChannels
	}
	o.NumChannels = o.Channels
	if o.FrameDuration == 0 {
		o.FrameDuration = defaultAudioFrameDuration
	}
	if o.StreamCapacity <= 0 {
		o.StreamCapacity = defaultAudioStreamCapacity
	}
	if o.SampleRate <= 0 || o.Channels <= 0 || o.FrameDuration <= 0 {
		return o, fmt.Errorf("%w: rate=%d channels=%d frame_duration=%s", ErrInvalidAudioFormat, o.SampleRate, o.Channels, o.FrameDuration)
	}
	if o.FrameDuration > time.Second {
		return o, fmt.Errorf("%w: frame duration must not exceed one second", ErrInvalidAudioFormat)
	}
	o.Format = strings.TrimSpace(o.Format)
	if o.Format != "" && !validAudioFormat(o.Format) {
		return o, fmt.Errorf("invalid audio format hint %q", o.Format)
	}
	return o, nil
}

func validAudioFormat(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '_' {
			continue
		}
		return false
	}
	return true
}

type AudioDecodeError struct {
	Path   string
	Codec  string
	Stderr string
	Cause  error
}

func (e *AudioDecodeError) Error() string {
	if e == nil {
		return "<nil>"
	}
	message := "decode audio file"
	if e.Path != "" {
		message += " " + e.Path
	}
	if e.Stderr != "" {
		message += ": " + e.Stderr
	} else if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

func (e *AudioDecodeError) Unwrap() error { return e.Cause }

type AudioFrameStream interface {
	stream.Reader[AudioFrame]
	Close() error
	Wait(context.Context) error
}

type audioFrameStream struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	out    *stream.Channel[AudioFrame]
	done   chan struct{}

	closeOnce sync.Once
	mu        sync.RWMutex
	err       error
}

func newAudioFrameStream(parent context.Context, capacity int) *audioFrameStream {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	return &audioFrameStream{ctx: ctx, cancel: cancel, out: stream.NewChannel[AudioFrame](capacity), done: make(chan struct{})}
}

func (s *audioFrameStream) Recv(ctx context.Context) (AudioFrame, error) { return s.out.Recv(ctx) }

func (s *audioFrameStream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel(stream.ErrClosed)
		_ = s.out.Abort(stream.ErrClosed)
	})
	return nil
}

func (s *audioFrameStream) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.done:
		s.mu.RLock()
		err := s.err
		s.mu.RUnlock()
		return err
	}
}

func (s *audioFrameStream) finish(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
	if err == nil || errors.Is(err, io.EOF) {
		_ = s.out.Close()
	} else {
		_ = s.out.Abort(err)
	}
	close(s.done)
}

func CalculateAudioDurationSeconds(frame AudioFrame) float64 { return frame.Duration().Seconds() }

// AudioFramesFromFile decodes a file to bounded 16-bit PCM frames. Matching
// PCM16 WAV files use the native streaming parser (fast startup, no subprocess);
// other formats and resampling use ffmpeg, matching agents-js's codec surface.
func AudioFramesFromFile(ctx context.Context, path string, options AudioDecodeOptions) (AudioFrameStream, error) {
	resolved, err := options.resolve()
	if err != nil {
		return nil, err
	}
	if path == "" {
		return nil, errors.New("audio file path is required")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, &AudioDecodeError{Path: path, Cause: err}
	}
	wav, wavErr := inspectPCM16WAV(file)
	nativeHint := resolved.Format == "" || strings.EqualFold(resolved.Format, "wav")
	if wavErr == nil && nativeHint && wav.sampleRate == resolved.SampleRate && wav.channels == resolved.Channels {
		return startNativeWAV(ctx, path, file, wav, resolved)
	}
	_ = file.Close()
	if resolved.Format != "" && strings.EqualFold(resolved.Format, "wav") && wavErr != nil {
		return nil, &AudioDecodeError{Path: path, Codec: "wav", Cause: wavErr}
	}
	return startFFmpegAudio(ctx, path, resolved)
}

// LoopAudioFramesFromFile repeatedly decodes a file without retaining the
// entire file in memory. An empty file terminates with io.ErrUnexpectedEOF
// instead of spinning at 100% CPU.
func LoopAudioFramesFromFile(ctx context.Context, path string, options AudioDecodeOptions) (AudioFrameStream, error) {
	resolved, err := options.resolve()
	if err != nil {
		return nil, err
	}
	first, err := AudioFramesFromFile(ctx, path, resolved)
	if err != nil {
		return nil, err
	}
	output := newAudioFrameStream(ctx, resolved.StreamCapacity)
	go func() {
		current := first
		defer func() { _ = current.Close() }()
		for {
			emitted := false
			for {
				frame, recvErr := current.Recv(output.ctx)
				if recvErr == nil {
					emitted = true
					if sendErr := output.out.Send(output.ctx, frame); sendErr != nil {
						output.finish(context.Cause(output.ctx))
						return
					}
					continue
				}
				if !errors.Is(recvErr, io.EOF) {
					if context.Cause(output.ctx) != nil {
						output.finish(context.Cause(output.ctx))
					} else {
						output.finish(recvErr)
					}
					return
				}
				break
			}
			_ = current.Close()
			if !emitted {
				output.finish(io.ErrUnexpectedEOF)
				return
			}
			if err := context.Cause(output.ctx); err != nil {
				output.finish(err)
				return
			}
			current, err = AudioFramesFromFile(output.ctx, path, resolved)
			if err != nil {
				output.finish(err)
				return
			}
		}
	}()
	return output, nil
}

type wavDescriptor struct {
	sampleRate int
	channels   int
	dataOffset int64
	dataBytes  int64
}

func inspectPCM16WAV(file *os.File) (wavDescriptor, error) {
	var result wavDescriptor
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return result, err
	}
	header := make([]byte, 12)
	if _, err := io.ReadFull(file, header); err != nil {
		return result, err
	}
	if string(header[:4]) != "RIFF" || string(header[8:]) != "WAVE" {
		return result, errors.New("not a RIFF/WAVE file")
	}
	stat, err := file.Stat()
	if err != nil {
		return result, err
	}
	var foundFormat, foundData bool
	for {
		chunkHeader := make([]byte, 8)
		if _, err := io.ReadFull(file, chunkHeader); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return result, err
		}
		chunkSize := int64(binary.LittleEndian.Uint32(chunkHeader[4:]))
		chunkStart, _ := file.Seek(0, io.SeekCurrent)
		chunkEnd := chunkStart + chunkSize
		if chunkEnd < chunkStart || chunkEnd > stat.Size() {
			return result, errors.New("WAV chunk exceeds file bounds")
		}
		switch string(chunkHeader[:4]) {
		case "fmt ":
			if chunkSize < 16 {
				return result, errors.New("WAV fmt chunk is too short")
			}
			format := make([]byte, 16)
			if _, err := io.ReadFull(file, format); err != nil {
				return result, err
			}
			if binary.LittleEndian.Uint16(format[0:]) != 1 || binary.LittleEndian.Uint16(format[14:]) != 16 {
				return result, errors.New("WAV is not signed 16-bit PCM")
			}
			result.channels = int(binary.LittleEndian.Uint16(format[2:]))
			result.sampleRate = int(binary.LittleEndian.Uint32(format[4:]))
			if result.channels <= 0 || result.sampleRate <= 0 {
				return result, ErrInvalidAudioFormat
			}
			foundFormat = true
		case "data":
			result.dataOffset, result.dataBytes = chunkStart, chunkSize
			foundData = true
		}
		next := chunkEnd + chunkSize%2
		if next > stat.Size() {
			return result, errors.New("WAV chunk padding exceeds file bounds")
		}
		if _, err := file.Seek(next, io.SeekStart); err != nil {
			return result, err
		}
		if foundFormat && foundData {
			break
		}
	}
	if !foundFormat || !foundData {
		return result, errors.New("WAV is missing fmt or data chunk")
	}
	if result.dataBytes%(int64(result.channels)*2) != 0 {
		return result, ErrIncompletePCMFrame
	}
	_, err = file.Seek(result.dataOffset, io.SeekStart)
	return result, err
}

func startNativeWAV(ctx context.Context, path string, file *os.File, descriptor wavDescriptor, options AudioDecodeOptions) (AudioFrameStream, error) {
	streamOutput := newAudioFrameStream(ctx, options.StreamCapacity)
	samplesPerChannel := max(1, int(time.Duration(options.SampleRate)*options.FrameDuration/time.Second))
	packetizer, err := NewAudioByteStream(options.SampleRate, options.Channels, samplesPerChannel)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	go func() {
		defer file.Close()
		limited := &io.LimitedReader{R: file, N: descriptor.dataBytes}
		buffer := make([]byte, 32<<10)
		for {
			n, readErr := limited.Read(buffer)
			if n != 0 {
				for _, frame := range packetizer.Write(buffer[:n]) {
					if err := streamOutput.out.Send(streamOutput.ctx, frame); err != nil {
						streamOutput.finish(context.Cause(streamOutput.ctx))
						return
					}
				}
			}
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					frames, flushErr := packetizer.Flush()
					if flushErr == nil {
						for _, frame := range frames {
							if err := streamOutput.out.Send(streamOutput.ctx, frame); err != nil {
								streamOutput.finish(context.Cause(streamOutput.ctx))
								return
							}
						}
					}
					streamOutput.finish(flushErr)
				} else {
					streamOutput.finish(&AudioDecodeError{Path: path, Codec: "wav", Cause: readErr})
				}
				return
			}
		}
	}()
	return streamOutput, nil
}

func startFFmpegAudio(ctx context.Context, path string, options AudioDecodeOptions) (AudioFrameStream, error) {
	ffmpeg := options.FFmpegPath
	if ffmpeg == "" {
		ffmpeg = ResolveFFmpegPath()
		if ffmpeg == "" {
			return nil, &AudioDecodeError{Path: path, Cause: errors.New("ffmpeg is required for compressed audio or resampling")}
		}
	}
	streamOutput := newAudioFrameStream(ctx, options.StreamCapacity)
	args := []string{"-v", "error", "-nostdin", "-probesize", "32", "-analyzeduration", "0", "-fflags", "+nobuffer+flush_packets", "-flags", "low_delay"}
	if options.Format != "" {
		args = append(args, "-f", options.Format)
	}
	args = append(args, "-i", path, "-f", "s16le", "-acodec", "pcm_s16le", "-ac", fmt.Sprint(options.Channels), "-ar", fmt.Sprint(options.SampleRate), "pipe:1")
	command := exec.CommandContext(streamOutput.ctx, ffmpeg, args...)
	stdout, err := command.StdoutPipe()
	if err != nil {
		return nil, &AudioDecodeError{Path: path, Cause: err}
	}
	stderr := &limitedBuffer{limit: maxFFmpegErrorBytes}
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		return nil, &AudioDecodeError{Path: path, Cause: err}
	}
	samplesPerChannel := max(1, int(time.Duration(options.SampleRate)*options.FrameDuration/time.Second))
	packetizer, err := NewAudioByteStream(options.SampleRate, options.Channels, samplesPerChannel)
	if err != nil {
		streamOutput.cancel(err)
		_ = command.Wait()
		return nil, err
	}
	go func() {
		buffer := make([]byte, 32<<10)
		var readErr error
		for {
			n, err := stdout.Read(buffer)
			if n != 0 {
				for _, frame := range packetizer.Write(buffer[:n]) {
					if sendErr := streamOutput.out.Send(streamOutput.ctx, frame); sendErr != nil {
						readErr = context.Cause(streamOutput.ctx)
						break
					}
				}
			}
			if readErr != nil || err != nil {
				if err != nil && !errors.Is(err, io.EOF) {
					readErr = err
				}
				break
			}
		}
		waitErr := command.Wait()
		if cause := context.Cause(streamOutput.ctx); cause != nil {
			streamOutput.finish(cause)
			return
		}
		if readErr != nil || waitErr != nil {
			streamOutput.finish(&AudioDecodeError{Path: path, Codec: options.Format, Stderr: strings.TrimSpace(stderr.String()), Cause: errors.Join(readErr, waitErr)})
			return
		}
		frames, flushErr := packetizer.Flush()
		if flushErr == nil {
			for _, frame := range frames {
				if err := streamOutput.out.Send(streamOutput.ctx, frame); err != nil {
					streamOutput.finish(context.Cause(streamOutput.ctx))
					return
				}
			}
		}
		streamOutput.finish(flushErr)
	}()
	return streamOutput, nil
}

type limitedBuffer struct {
	mu    sync.Mutex
	limit int
	data  bytes.Buffer
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	original := len(p)
	remaining := b.limit - b.data.Len()
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		_, _ = b.data.Write(p)
	}
	return original, nil
}

func (b *limitedBuffer) String() string {
	b.mu.Lock()
	value := b.data.String()
	b.mu.Unlock()
	return value
}
