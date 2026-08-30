// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"fmt"

	agents "github.com/infinityscroll/livekit-agents-go"
	mediabase "github.com/livekit/media-sdk"
)

// convertAudioFrame converts mono/stereo interleaved PCM while retaining the
// media SDK's predictable streaming resampler. Conversion happens only when a
// producer's format differs from the published track.
func convertAudioFrame(frame agents.AudioFrame, sampleRate, channels int) (agents.AudioFrame, error) {
	if frame.SampleRate <= 0 || frame.Channels < 1 || frame.Channels > 2 || len(frame.Data)%frame.Channels != 0 || frame.SamplesPerChannel != len(frame.Data)/frame.Channels {
		return agents.AudioFrame{}, fmt.Errorf("%w: rate=%d channels=%d samples=%d", agents.ErrInvalidAudioFormat, frame.SampleRate, frame.Channels, len(frame.Data))
	}
	if sampleRate <= 0 || channels < 1 || channels > 2 {
		return agents.AudioFrame{}, fmt.Errorf("%w: target rate=%d channels=%d", agents.ErrInvalidAudioFormat, sampleRate, channels)
	}
	if frame.SampleRate == sampleRate && frame.Channels == channels {
		copyFrame := frame
		copyFrame.Data = append([]int16(nil), frame.Data...)
		return copyFrame, nil
	}

	perChannel := make([][]int16, frame.Channels)
	for channel := range perChannel {
		perChannel[channel] = make([]int16, frame.SamplesPerChannel)
		for sample := range frame.SamplesPerChannel {
			perChannel[channel][sample] = frame.Data[sample*frame.Channels+channel]
		}
	}
	if frame.Channels == 2 && channels == 1 {
		mono := make([]int16, frame.SamplesPerChannel)
		for sample := range mono {
			mono[sample] = int16((int32(perChannel[0][sample]) + int32(perChannel[1][sample])) / 2)
		}
		perChannel = [][]int16{mono}
	} else if frame.Channels == 1 && channels == 2 {
		left := perChannel[0]
		perChannel = [][]int16{left, append([]int16(nil), left...)}
	}

	for channel := range perChannel {
		perChannel[channel] = mediabase.Resample(nil, sampleRate, mediabase.PCM16Sample(perChannel[channel]), frame.SampleRate, mediabase.WithPredictableResample(true))
	}
	samples := len(perChannel[0])
	data := make([]int16, samples*channels)
	for sample := range samples {
		for channel := range channels {
			data[sample*channels+channel] = perChannel[channel][sample]
		}
	}
	converted, err := agents.NewAudioFrame(data, sampleRate, channels)
	if err != nil {
		return agents.AudioFrame{}, err
	}
	converted.UserData = frame.UserData
	return converted, nil
}
