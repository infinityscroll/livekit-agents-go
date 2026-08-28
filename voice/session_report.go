// SPDX-License-Identifier: Apache-2.0

package voice

// ReportOptions returns the immutable session configuration used by the
// cross-SDK SessionReport contract. Recording is supplied by the job/session
// registration layer because it can inherit dispatch policy and be demoted for
// a secondary session.
func (s *AgentSession[UserData]) ReportOptions(recording RecordingOptions) ReportSessionOptions {
	if s == nil {
		return ReportSessionOptions{Recording: recording}
	}
	s.mu.RLock()
	result := ReportSessionOptions{
		Interruption:         cloneReportInterruptionOptions(s.opts.turn.Interruption),
		Endpointing:          s.opts.turn.Endpointing,
		MaxToolSteps:         s.opts.maxToolSteps,
		PreemptiveGeneration: s.opts.turn.PreemptiveGeneration,
		Recording:            recording,
	}
	if s.opts.userAwayEnabled {
		value := s.opts.userAwayTimeout
		result.UserAwayTimeout = &value
	}
	s.mu.RUnlock()
	return result
}

func cloneReportInterruptionOptions(options InterruptionOptions) InterruptionOptions {
	result := options
	// Runtime model objects are neither immutable report configuration nor part
	// of the cross-SDK wire contract.
	result.Detector = nil
	if options.FalseInterruptionTimeout != nil {
		value := *options.FalseInterruptionTimeout
		result.FalseInterruptionTimeout = &value
	}
	if options.BackchannelBoundary != nil {
		value := *options.BackchannelBoundary
		result.BackchannelBoundary = &value
	}
	return result
}
