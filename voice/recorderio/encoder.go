// SPDX-License-Identifier: Apache-2.0

package recorderio

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os/exec"
	"strconv"
	"sync"

	agents "github.com/infinityscroll/livekit-agents-go"
)

func (r *RecorderIO) runEncoder(ctx context.Context, queue *batchQueue, outputPath string) {
	defer close(r.encodeDone)
	inputConverter := newMonoConverter(r.options.sampleRate, r.options.maxQueuedBytes/4)
	outputConverter := newMonoConverter(r.options.sampleRate, r.options.maxQueuedBytes/4)
	var encoder PCMEncoder
	var runErr error

	write := func(left, right []float32) error {
		pcm, err := interleavePCM16(left, right)
		if err != nil || len(pcm) == 0 {
			return err
		}
		if encoder == nil {
			encoder, err = r.options.encoderFactory(ctx, outputPath, r.options.sampleRate)
			if err != nil {
				return err
			}
			if isNil(encoder) {
				return ErrEncoderUnavailable
			}
		}
		return encoder.WritePCM(ctx, pcm)
	}

	for {
		batch, err := queue.Recv(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			runErr = err
			break
		}
		left, err := inputConverter.push(batch.input, false)
		if err != nil {
			runErr = err
			break
		}
		right, err := outputConverter.push(batch.output, len(batch.output) != 0)
		if err != nil {
			runErr = err
			break
		}
		if err := write(left, right); err != nil {
			runErr = err
			break
		}
	}
	if runErr == nil {
		left, leftErr := inputConverter.push(nil, true)
		right, rightErr := outputConverter.push(nil, true)
		runErr = errors.Join(leftErr, rightErr)
		if runErr == nil {
			runErr = write(left, right)
		}
	}
	if runErr != nil {
		_ = queue.Abort(runErr)
	}
	if encoder != nil {
		runErr = errors.Join(runErr, encoder.Close(ctx))
	}
	if runErr != nil && !(errors.Is(runErr, context.Canceled) && errors.Is(context.Cause(ctx), ErrClosed)) {
		r.reportError(runErr)
	}
	r.mu.Lock()
	r.encodeErr = runErr
	r.mu.Unlock()
}

type monoConverter struct {
	targetRate int
	maxSamples int
	rate       int
	channels   int
	resampler  *floatResampler
}

func newMonoConverter(targetRate, maxSamples int) *monoConverter {
	return &monoConverter{targetRate: targetRate, maxSamples: max(1, maxSamples)}
}

func (c *monoConverter) push(frames []agents.AudioFrame, flush bool) ([]float32, error) {
	var output []float32
	appendSamples := func(samples []float32) error {
		if len(samples) > c.maxSamples-len(output) {
			return ErrBufferLimit
		}
		output = append(output, samples...)
		return nil
	}
	for _, frame := range frames {
		if err := validateFrame(frame); err != nil {
			return nil, err
		}
		if frame.SamplesPerChannel == 0 {
			continue
		}
		estimated := int64(frame.SamplesPerChannel)
		if frame.SampleRate != c.targetRate {
			estimated = (estimated*int64(c.targetRate)+int64(frame.SampleRate)-1)/int64(frame.SampleRate) + 2
		}
		if estimated > int64(c.maxSamples-len(output)) {
			return nil, ErrBufferLimit
		}
		if c.rate != 0 && (c.rate != frame.SampleRate || c.channels != frame.Channels) {
			if c.resampler != nil {
				if err := appendSamples(c.resampler.push(nil, true)); err != nil {
					return nil, err
				}
			}
			c.rate, c.channels, c.resampler = 0, 0, nil
		}
		if c.rate == 0 {
			c.rate, c.channels = frame.SampleRate, frame.Channels
			if c.rate != c.targetRate {
				c.resampler = newFloatResampler(c.rate, c.targetRate)
			}
		}
		mono := downmixFloat(frame)
		if c.resampler != nil {
			mono = c.resampler.push(mono, false)
		}
		if err := appendSamples(mono); err != nil {
			return nil, err
		}
	}
	if flush && c.resampler != nil {
		if err := appendSamples(c.resampler.push(nil, true)); err != nil {
			return nil, err
		}
		// A flush is a synchronization boundary. Reset the interpolation
		// origin so a rounded fractional position from the previous segment
		// cannot refer to a sample that has already been discarded.
		c.resampler = newFloatResampler(c.rate, c.targetRate)
	}
	return output, nil
}

func downmixFloat(frame agents.AudioFrame) []float32 {
	result := make([]float32, frame.SamplesPerChannel)
	const inverseInt16 = 1.0 / 32768.0
	switch frame.Channels {
	case 1:
		for sample := range result {
			result[sample] = float32(frame.Data[sample]) * inverseInt16
		}
		return result
	case 2:
		for sample := range result {
			base := sample * 2
			result[sample] = float32((float64(frame.Data[base]) + float64(frame.Data[base+1])) * .5 * inverseInt16)
		}
		return result
	}
	for sample := range result {
		var sum float64
		base := sample * frame.Channels
		for channel := 0; channel < frame.Channels; channel++ {
			sum += float64(frame.Data[base+channel])
		}
		result[sample] = float32(sum / float64(frame.Channels) * inverseInt16)
	}
	return result
}

type floatResampler struct {
	inRate, outRate int64
	base, totalIn   int64
	nextOut         int64
	buffer          []float32
}

func newFloatResampler(inputRate, outputRate int) *floatResampler {
	return &floatResampler{inRate: int64(inputRate), outRate: int64(outputRate)}
}

func (r *floatResampler) push(input []float32, flush bool) []float32 {
	r.buffer = append(r.buffer, input...)
	r.totalIn += int64(len(input))
	estimated := int((r.totalIn*r.outRate)/r.inRate - r.nextOut)
	if estimated < 0 {
		estimated = 0
	}
	output := make([]float32, 0, estimated)
	limit := int64(-1)
	if flush {
		limit = (r.totalIn*r.outRate + r.inRate/2) / r.inRate
	}
	for {
		if flush && r.nextOut >= limit {
			break
		}
		position := r.nextOut * r.inRate
		left, fraction := position/r.outRate, position%r.outRate
		if left >= r.totalIn {
			break
		}
		right := left
		if fraction != 0 {
			right++
			if right >= r.totalIn {
				if !flush {
					break
				}
				right = left
			}
		}
		leftIndex, rightIndex := int(left-r.base), int(right-r.base)
		if leftIndex < 0 || rightIndex >= len(r.buffer) {
			break
		}
		value := (float64(r.buffer[leftIndex])*float64(r.outRate-fraction) + float64(r.buffer[rightIndex])*float64(fraction)) / float64(r.outRate)
		output = append(output, float32(value))
		r.nextOut++
	}
	needed := (r.nextOut * r.inRate) / r.outRate
	if needed > r.base {
		drop := min(needed-r.base, int64(len(r.buffer)))
		copy(r.buffer, r.buffer[int(drop):])
		r.buffer = r.buffer[:len(r.buffer)-int(drop)]
		r.base += drop
	}
	if flush {
		r.buffer = r.buffer[:0]
		r.base = r.totalIn
	}
	return output
}

func interleavePCM16(left, right []float32) ([]byte, error) {
	length := max(len(left), len(right))
	if length > int(^uint(0)>>1)/4 {
		return nil, ErrBufferLimit
	}
	data := make([]byte, length*4)
	leftOffset, rightOffset := length-len(left), length-len(right)
	for index := 0; index < length; index++ {
		var leftSample, rightSample float32
		if index >= leftOffset {
			leftSample = left[index-leftOffset]
		}
		if index >= rightOffset {
			rightSample = right[index-rightOffset]
		}
		binary.LittleEndian.PutUint16(data[index*4:], uint16(floatToPCM16(leftSample)))
		binary.LittleEndian.PutUint16(data[index*4+2:], uint16(floatToPCM16(rightSample)))
	}
	return data, nil
}

func floatToPCM16(value float32) int16 {
	scaled := float64(value) * 32768
	if scaled > math.MaxInt16 {
		scaled = math.MaxInt16
	} else if scaled < math.MinInt16 {
		scaled = math.MinInt16
	}
	return int16(math.Floor(scaled + .5))
}

type ffmpegEncoder struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	done   chan error
	stderr *limitedBuffer
	once   sync.Once
}

func newFFmpegEncoder(ctx context.Context, outputPath string, sampleRate int) (PCMEncoder, error) {
	command := agents.FFmpegCommand(ctx,
		"-hide_banner", "-loglevel", "error", "-f", "s16le", "-ar", strconv.Itoa(sampleRate), "-ac", "2", "-i", "pipe:0",
		"-vn", "-c:a", "libopus", "-ac", "2", "-ar", strconv.Itoa(sampleRate), "-f", "ogg", "-y", outputPath,
	)
	stdin, err := command.StdinPipe()
	if err != nil {
		return nil, err
	}
	stderr := &limitedBuffer{limit: 64 << 10}
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		_ = stdin.Close()
		return nil, fmt.Errorf("recorderio: start ffmpeg: %w", err)
	}
	encoder := &ffmpegEncoder{cmd: command, stdin: stdin, done: make(chan error, 1), stderr: stderr}
	go func() {
		encoder.done <- command.Wait()
		close(encoder.done)
	}()
	return encoder, nil
}

func (e *ffmpegEncoder) WritePCM(ctx context.Context, pcm []byte) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	for len(pcm) != 0 {
		written, err := e.stdin.Write(pcm)
		if err != nil {
			return e.withStderr(err)
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		pcm = pcm[written:]
	}
	return context.Cause(ctx)
}

func (e *ffmpegEncoder) Close(ctx context.Context) error {
	e.once.Do(func() { _ = e.stdin.Close() })
	select {
	case err := <-e.done:
		return e.withStderr(err)
	case <-ctx.Done():
		if e.cmd.Process != nil {
			_ = e.cmd.Process.Kill()
		}
		err := <-e.done
		return errors.Join(context.Cause(ctx), e.withStderr(err))
	}
}

func (e *ffmpegEncoder) withStderr(err error) error {
	if err == nil {
		return nil
	}
	message := e.stderr.String()
	if message == "" {
		return err
	}
	return fmt.Errorf("%w: %s", err, message)
}

type limitedBuffer struct {
	buffer bytes.Buffer
	limit  int
}

func (b *limitedBuffer) Write(value []byte) (int, error) {
	original := len(value)
	if b.buffer.Len() < b.limit {
		value = value[:min(len(value), b.limit-b.buffer.Len())]
		_, _ = b.buffer.Write(value)
	}
	return original, nil
}

func (b *limitedBuffer) String() string { return b.buffer.String() }

var _ PCMEncoder = (*ffmpegEncoder)(nil)
