// SPDX-License-Identifier: Apache-2.0

package agents

import "time"

type TimedString struct {
	Text            string
	StartTime       *time.Duration
	EndTime         *time.Duration
	Confidence      *float64
	StartTimeOffset *time.Duration
	SpeakerID       *string
}

func NewTimedString(text string, start, end time.Duration) TimedString {
	return TimedString{Text: text, StartTime: &start, EndTime: &end}
}

type TimedStringOptions struct {
	Text            string
	StartTime       *time.Duration
	EndTime         *time.Duration
	Confidence      *float64
	StartTimeOffset *time.Duration
	SpeakerID       *string
}

func CreateTimedString(options TimedStringOptions) TimedString {
	return TimedString(options)
}
