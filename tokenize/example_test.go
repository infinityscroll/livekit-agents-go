// SPDX-License-Identifier: Apache-2.0

package tokenize_test

import (
	"fmt"

	"github.com/infinityscroll/livekit-agents-go/tokenize"
)

func ExampleSentenceTokenizer() {
	tokenizer := tokenize.NewSentenceTokenizer(tokenize.SentenceOptions{MinSentenceLength: 1})
	for _, sentence := range tokenizer.Tokenize("Hello! How are you?") {
		fmt.Println(sentence)
	}
	// Output:
	// Hello!
	// How are you?
}

func ExampleHyphenateWord() {
	fmt.Println(tokenize.HyphenateWord("communication"))
	// Output: [com mu ni ca tion]
}
