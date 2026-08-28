// SPDX-License-Identifier: Apache-2.0

package providerformat

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/livekit/agents-go/llm"
)

type serializedImage struct {
	Detail      llm.ImageDetail
	MIMEType    string
	Base64Data  string
	ExternalURL string
}

var supportedImageMIMEs = map[string]struct{}{
	"image/jpeg": {},
	"image/png":  {},
	"image/webp": {},
	"image/gif":  {},
}

func serializeImage(image llm.ImageContent) (serializedImage, error) {
	detail := image.InferenceDetail
	if detail == "" {
		detail = llm.ImageDetailAuto
	}
	switch value := image.Image.(type) {
	case string:
		if !strings.HasPrefix(value, "data:") {
			return serializedImage{Detail: detail, MIMEType: image.MIMEType, ExternalURL: value}, nil
		}
		header, data, ok := strings.Cut(value, ",")
		if !ok {
			return serializedImage{}, errors.New("provider format: invalid image data URL")
		}
		mediaHeader := strings.TrimPrefix(header, "data:")
		headerMIME, _, _ := strings.Cut(mediaHeader, ";")
		if headerMIME == "" {
			return serializedImage{}, errors.New("provider format: image data URL has no MIME type")
		}
		mimeType := headerMIME
		if image.MIMEType != "" {
			mimeType = image.MIMEType
		}
		if err := validateImageMIME(mimeType); err != nil {
			return serializedImage{}, err
		}
		if _, err := base64.StdEncoding.DecodeString(data); err != nil {
			return serializedImage{}, fmt.Errorf("provider format: invalid base64 image data: %w", err)
		}
		return serializedImage{Detail: detail, MIMEType: mimeType, Base64Data: data}, nil
	case []byte:
		if len(value) == 0 {
			return serializedImage{}, errors.New("provider format: image data is empty")
		}
		mimeType := image.MIMEType
		if mimeType == "" {
			mimeType = http.DetectContentType(value)
		}
		if err := validateImageMIME(mimeType); err != nil {
			return serializedImage{}, err
		}
		return serializedImage{Detail: detail, MIMEType: mimeType, Base64Data: base64.StdEncoding.EncodeToString(value)}, nil
	default:
		return serializedImage{}, fmt.Errorf("provider format: unsupported image value %T; use URL, data URL, or encoded []byte", image.Image)
	}
}

func validateImageMIME(mimeType string) error {
	if _, ok := supportedImageMIMEs[mimeType]; !ok {
		return fmt.Errorf("provider format: unsupported image MIME type %q (want jpeg, png, webp, or gif)", mimeType)
	}
	return nil
}

func openAIImage(image llm.ImageContent, responses bool) (map[string]any, error) {
	serialized, err := serializeImage(image)
	if err != nil {
		return nil, err
	}
	url := serialized.ExternalURL
	if url == "" {
		url = "data:" + serialized.MIMEType + ";base64," + serialized.Base64Data
	}
	if responses {
		return map[string]any{"type": "input_image", "image_url": url, "detail": string(serialized.Detail)}, nil
	}
	return map[string]any{
		"type":      "image_url",
		"image_url": map[string]any{"url": url, "detail": string(serialized.Detail)},
	}, nil
}

func googleImage(image llm.ImageContent) (map[string]any, error) {
	serialized, err := serializeImage(image)
	if err != nil {
		return nil, err
	}
	if serialized.ExternalURL != "" {
		mimeType := serialized.MIMEType
		if mimeType == "" {
			mimeType = "image/jpeg"
		}
		return map[string]any{"fileData": map[string]any{"fileUri": serialized.ExternalURL, "mimeType": mimeType}}, nil
	}
	return map[string]any{"inlineData": map[string]any{"data": serialized.Base64Data, "mimeType": serialized.MIMEType}}, nil
}
