// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"strings"
	"sync"
)

type LanguageCode string

var KnownLanguageCodes = []LanguageCode{
	"af", "am", "ar", "as", "az", "be", "bg", "bn", "bs", "ca", "cs", "cy", "da", "de",
	"el", "en", "es", "et", "eu", "fa", "ff", "fi", "fr", "ga", "gl", "gu", "ha", "he",
	"hi", "hr", "hu", "hy", "id", "ig", "is", "it", "ja", "jv", "ka", "kk", "km", "kn",
	"ko", "ku", "ky", "lb", "lg", "ln", "lo", "lt", "lv", "mi", "mk", "ml", "mn", "mr",
	"ms", "mt", "my", "ne", "nl", "no", "ny", "oc", "or", "pa", "pl", "ps", "pt", "ro",
	"ru", "sd", "sk", "sl", "sn", "so", "sq", "sr", "sv", "sw", "ta", "te", "tg", "th",
	"tl", "tr", "uk", "ur", "uz", "vi", "wo", "xh", "yo", "zh", "zu",
}

func AsLanguageCode(language string) LanguageCode { return LanguageCode(language) }

var iso6393To1 = sync.OnceValue(func() map[string]string {
	return map[string]string{
		"afr": "af", "amh": "am", "ara": "ar", "hye": "hy", "asm": "as", "aze": "az",
		"bel": "be", "ben": "bn", "bos": "bs", "bul": "bg", "mya": "my", "cat": "ca",
		"cmn": "zh", "nya": "ny", "hrv": "hr", "ces": "cs", "dan": "da", "nld": "nl",
		"eng": "en", "est": "et", "fin": "fi", "fra": "fr", "ful": "ff", "glg": "gl",
		"lug": "lg", "kat": "ka", "deu": "de", "ell": "el", "guj": "gu", "hau": "ha",
		"heb": "he", "hin": "hi", "hun": "hu", "isl": "is", "ibo": "ig", "ind": "id",
		"gle": "ga", "ita": "it", "jpn": "ja", "jav": "jv", "kan": "kn", "kaz": "kk",
		"khm": "km", "kor": "ko", "kur": "ku", "kir": "ky", "lao": "lo", "lav": "lv",
		"lin": "ln", "lit": "lt", "ltz": "lb", "mkd": "mk", "msa": "ms", "mal": "ml",
		"mlt": "mt", "zho": "zh", "mri": "mi", "mar": "mr", "mon": "mn", "nep": "ne",
		"nor": "no", "oci": "oc", "ori": "or", "pus": "ps", "fas": "fa", "pol": "pl",
		"por": "pt", "pan": "pa", "ron": "ro", "rus": "ru", "srp": "sr", "sna": "sn",
		"snd": "sd", "slk": "sk", "slv": "sl", "som": "so", "spa": "es", "swa": "sw",
		"swe": "sv", "tam": "ta", "tgk": "tg", "tel": "te", "tha": "th", "tur": "tr",
		"ukr": "uk", "urd": "ur", "uzb": "uz", "vie": "vi", "cym": "cy", "wol": "wo",
		"xho": "xh", "zul": "zu",
	}
})

var languageNamesToCode = sync.OnceValue(func() map[string]string {
	return map[string]string{
		"afrikaans": "af", "albanian": "sq", "amharic": "am", "arabic": "ar", "armenian": "hy",
		"azerbaijani": "az", "basque": "eu", "belarusian": "be", "bengali": "bn", "bosnian": "bs",
		"bulgarian": "bg", "burmese": "my", "catalan": "ca", "chinese": "zh", "croatian": "hr",
		"czech": "cs", "danish": "da", "dutch": "nl", "english": "en", "estonian": "et",
		"finnish": "fi", "french": "fr", "galician": "gl", "georgian": "ka", "german": "de",
		"greek": "el", "gujarati": "gu", "hausa": "ha", "hebrew": "he", "hindi": "hi",
		"hungarian": "hu", "icelandic": "is", "indonesian": "id", "irish": "ga", "italian": "it",
		"japanese": "ja", "javanese": "jv", "kannada": "kn", "kazakh": "kk", "khmer": "km",
		"korean": "ko", "kurdish": "ku", "kyrgyz": "ky", "lao": "lo", "latvian": "lv",
		"lingala": "ln", "lithuanian": "lt", "luxembourgish": "lb", "macedonian": "mk",
		"malay": "ms", "malayalam": "ml", "maltese": "mt", "maori": "mi", "marathi": "mr",
		"mongolian": "mn", "nepali": "ne", "norwegian": "no", "occitan": "oc", "oriya": "or",
		"pashto": "ps", "persian": "fa", "polish": "pl", "portuguese": "pt", "punjabi": "pa",
		"romanian": "ro", "russian": "ru", "serbian": "sr", "shona": "sn", "sindhi": "sd",
		"slovak": "sk", "slovene": "sl", "slovenian": "sl", "somali": "so", "spanish": "es",
		"swahili": "sw", "swedish": "sv", "tagalog": "tl", "tamil": "ta", "tajik": "tg",
		"telugu": "te", "thai": "th", "turkish": "tr", "ukrainian": "uk", "urdu": "ur",
		"uzbek": "uz", "vietnamese": "vi", "welsh": "cy", "wolof": "wo", "xhosa": "xh",
		"yoruba": "yo", "zulu": "zu",
	}
})

var codeToLanguageName = sync.OnceValue(func() map[string]string {
	names := languageNamesToCode()
	result := make(map[string]string, len(names))
	for name, code := range names {
		result[code] = name
	}
	result["sl"] = "slovene"
	return result
})

func NormalizeLanguage(language string) LanguageCode {
	lowered := strings.ToLower(strings.TrimSpace(language))
	if lowered == "" {
		return ""
	}
	if code, ok := languageNamesToCode()[lowered]; ok {
		return LanguageCode(code)
	}
	if code, ok := iso6393To1()[lowered]; ok {
		return LanguageCode(code)
	}
	parts := strings.Split(strings.ReplaceAll(lowered, "_", "-"), "-")
	if len(parts) >= 2 {
		for i := 1; i < len(parts); i++ {
			if len(parts[i]) == 4 {
				parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
			} else {
				parts[i] = strings.ToUpper(parts[i])
			}
		}
		return LanguageCode(strings.Join(parts, "-"))
	}
	return LanguageCode(lowered)
}

func BaseLanguage(language string) string {
	normalized := string(NormalizeLanguage(language))
	base, _, _ := strings.Cut(normalized, "-")
	if code, ok := iso6393To1()[base]; ok {
		return code
	}
	return base
}

func LanguageRegion(language string) (string, bool) {
	parts := strings.Split(string(NormalizeLanguage(language)), "-")
	for _, part := range parts[1:] {
		if len(part) == 2 {
			return part, true
		}
	}
	return "", false
}

func ISOLanguage(language string) string {
	base := BaseLanguage(language)
	if region, ok := LanguageRegion(language); ok {
		return base + "-" + region
	}
	return base
}

func LanguageName(language string) (string, bool) {
	name, ok := codeToLanguageName()[BaseLanguage(language)]
	return name, ok
}

func AreLanguagesEquivalent(left, right string) bool {
	return NormalizeLanguage(left) == NormalizeLanguage(right)
}
