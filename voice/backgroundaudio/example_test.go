// SPDX-License-Identifier: Apache-2.0

package backgroundaudio_test

import (
	"context"
	"fmt"

	backgroundaudio "github.com/infinityscroll/livekit-agents-go/voice/backgroundaudio"
)

func ExampleNewBackgroundAudioPlayer() {
	player, err := backgroundaudio.NewBackgroundAudioPlayer(backgroundaudio.BackgroundAudioPlayerOptions{
		AmbientSound: backgroundaudio.ConfiguredSound(
			backgroundaudio.Config(backgroundaudio.Builtin(backgroundaudio.OFFICE_AMBIENCE)).WithVolume(.8),
		),
		ThinkingSound: backgroundaudio.Choose(
			backgroundaudio.Config(backgroundaudio.Builtin(backgroundaudio.KEYBOARD_TYPING)).WithProbability(.7),
			backgroundaudio.Config(backgroundaudio.Builtin(backgroundaudio.KEYBOARD_TYPING2)).WithProbability(.3),
		),
	})
	if err != nil {
		panic(err)
	}
	defer player.Close(context.Background())

	fmt.Println("background audio configured")
	// Output: background audio configured
}
