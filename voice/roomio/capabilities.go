// SPDX-License-Identifier: Apache-2.0

package roomio

// SDKCapabilities makes the pinned server-sdk-go v2.18.1 bridge limitations
// inspectable instead of silently degrading behavior.
type SDKCapabilities struct {
	RawDecodedVideo            bool
	CancelableTextReader       bool
	BoundedTextReaderBuffer    bool
	CancelableTextWriter       bool
	CancelableTextWriterClose  bool
	TextSenderIdentityOverride bool
	LegacyTranscriptionPublish bool
	CancelableTrackPublication bool
	CancelableRPC              bool
	RemoteDecoderCompletion    bool
}

// Capabilities reports features exposed by the RTC SDK version this adapter
// targets. False fields have explicit fallbacks or errors documented on the
// corresponding RoomIO method.
func Capabilities() SDKCapabilities {
	return SDKCapabilities{
		RawDecodedVideo:            false,
		CancelableTextReader:       false,
		BoundedTextReaderBuffer:    false,
		CancelableTextWriter:       false,
		CancelableTextWriterClose:  false,
		TextSenderIdentityOverride: false,
		LegacyTranscriptionPublish: false,
		CancelableTrackPublication: false,
		CancelableRPC:              false,
		RemoteDecoderCompletion:    false,
	}
}
