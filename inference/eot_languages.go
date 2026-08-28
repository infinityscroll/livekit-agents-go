// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"strings"
	"sync"

	agents "github.com/livekit/agents-go"
)

// TurnDetectorModel is the model identifier reported in metrics and usage.
type TurnDetectorModel string

const (
	TurnDetectorModelV1     TurnDetectorModel = "turn-detector-v1"
	TurnDetectorModelV1Mini TurnDetectorModel = "turn-detector-v1-mini"
)

// TurnDetectorVersion is the public model selection option.
type TurnDetectorVersion string

const (
	TurnDetectorV1     TurnDetectorVersion = "v1"
	TurnDetectorV1Mini TurnDetectorVersion = "v1-mini"
)

func defaultLocalTurnThresholds() map[string]float64 {
	return map[string]float64{
		"ar": 0.3500, "de": 0.2450, "en": 0.3600, "es": 0.3500,
		"fr": 0.2850, "hi": 0.3050, "id": 0.3450, "it": 0.2300,
		"ja": 0.2950, "ko": 0.4000, "nl": 0.2000, "pt": 0.3200,
		"tr": 0.2550, "zh": 0.3550,
	}
}

// localTurnThreshold uses a static switch so detector behavior has no mutable
// process-global map dependency. The public snapshot below remains mutable for
// compatibility, but changing it cannot alter active calibration.
func localTurnThreshold(language string) (float64, bool) {
	switch language {
	case "ar":
		return 0.3500, true
	case "de":
		return 0.2450, true
	case "en":
		return 0.3600, true
	case "es":
		return 0.3500, true
	case "fr":
		return 0.2850, true
	case "hi":
		return 0.3050, true
	case "id":
		return 0.3450, true
	case "it":
		return 0.2300, true
	case "ja":
		return 0.2950, true
	case "ko":
		return 0.4000, true
	case "nl":
		return 0.2000, true
	case "pt":
		return 0.3200, true
	case "tr":
		return 0.2550, true
	case "zh":
		return 0.3550, true
	default:
		return 0, false
	}
}

// LocalTurnThresholds is a compatibility snapshot of the calibrated mini
// model table. Mutating it does not affect detector behavior.
var LocalTurnThresholds = defaultLocalTurnThresholds()

// ThresholdOverride represents the scalar-or-language-map union exposed by
// agents-js. A scalar applies to every language. Language values are layered
// over calibrated defaults. The zero value means no override.
//
// Use ScalarThreshold when zero is an intentional override; Scalar is a
// pointer specifically to preserve unset versus an explicit zero.
type ThresholdOverride struct {
	Scalar    *float64           `json:"-"`
	Languages map[string]float64 `json:"-"`
}

func (o ThresholdOverride) IsZero() bool { return !o.set() }

// MarshalJSON preserves the scalar-or-map union used by the TypeScript and
// Python SDKs instead of leaking the Go representation into config snapshots.
func (o ThresholdOverride) MarshalJSON() ([]byte, error) {
	if o.Scalar != nil {
		return json.Marshal(*o.Scalar)
	}
	if o.Languages != nil {
		return json.Marshal(o.Languages)
	}
	return []byte("null"), nil
}

// ScalarThreshold applies one threshold to every language, including zero.
func ScalarThreshold(value float64) ThresholdOverride {
	return ThresholdOverride{Scalar: &value}
}

// LanguageThresholds returns an owned, normalized-on-use threshold map.
func LanguageThresholds(values map[string]float64) ThresholdOverride {
	return ThresholdOverride{Languages: maps.Clone(values)}
}

func (o ThresholdOverride) set() bool { return o.Scalar != nil || o.Languages != nil }

func normalizeThresholdOverride(value ThresholdOverride) (ThresholdOverride, error) {
	if value.Scalar != nil && value.Languages != nil {
		return ThresholdOverride{}, fmt.Errorf("threshold override cannot contain both Scalar and Languages")
	}
	if value.Scalar != nil {
		if math.IsNaN(*value.Scalar) || math.IsInf(*value.Scalar, 0) {
			return ThresholdOverride{}, fmt.Errorf("threshold scalar must be finite")
		}
		copy := *value.Scalar
		return ThresholdOverride{Scalar: &copy}, nil
	}
	if value.Languages == nil {
		return ThresholdOverride{}, nil
	}
	result := make(map[string]float64, len(value.Languages))
	for language, threshold := range value.Languages {
		if math.IsNaN(threshold) || math.IsInf(threshold, 0) {
			return ThresholdOverride{}, fmt.Errorf("threshold for %q must be finite", language)
		}
		result[normalizeTurnLanguage(language)] = threshold
	}
	return ThresholdOverride{Languages: result}, nil
}

func cloneThresholdOverride(value ThresholdOverride) ThresholdOverride {
	result := ThresholdOverride{Languages: maps.Clone(value.Languages)}
	if value.Scalar != nil {
		copy := *value.Scalar
		result.Scalar = &copy
	}
	return result
}

func normalizeTurnLanguage(language string) string {
	language = strings.TrimSpace(language)
	if language == "" {
		return "en"
	}
	if strings.EqualFold(language, "mandarin") {
		return "zh"
	}
	return agents.BaseLanguage(language)
}

// ThresholdOptions resolves the three threshold layers used by the audio turn
// detector: caller overrides, cloud/shipped defaults, and the materialized
// lookup table. It is safe for concurrent reads and runtime updates.
type ThresholdOptions struct {
	mu sync.RWMutex

	model                TurnDetectorModel
	overrides            ThresholdOverride
	backchannelOverrides ThresholdOverride

	serverThresholds map[string]float64
	serverDefault    *float64
	serverBC         map[string]float64
	serverBCDefault  *float64

	thresholds map[string]float64
	defaultT   *float64
	bc         map[string]float64
	bcDefault  *float64
}

func NewThresholdOptions(model TurnDetectorModel, unlikely, backchannel ThresholdOverride) (*ThresholdOptions, error) {
	if model != TurnDetectorModelV1 && model != TurnDetectorModelV1Mini {
		return nil, fmt.Errorf("unknown turn detector model %q", model)
	}
	unlikely, err := normalizeThresholdOverride(unlikely)
	if err != nil {
		return nil, fmt.Errorf("unlikely threshold: %w", err)
	}
	backchannel, err = normalizeThresholdOverride(backchannel)
	if err != nil {
		return nil, fmt.Errorf("backchannel threshold: %w", err)
	}
	result := &ThresholdOptions{
		model: model, overrides: unlikely, backchannelOverrides: backchannel,
	}
	if model == TurnDetectorModelV1Mini {
		result.serverThresholds = defaultLocalTurnThresholds()
		value, _ := localTurnThreshold("en")
		result.serverDefault = &value
	}
	result.resolveLocked()
	return result, nil
}

func (o *ThresholdOptions) Model() TurnDetectorModel {
	o.mu.RLock()
	value := o.model
	o.mu.RUnlock()
	return value
}

func (o *ThresholdOptions) Overrides() ThresholdOverride {
	o.mu.RLock()
	value := cloneThresholdOverride(o.overrides)
	o.mu.RUnlock()
	return value
}

func (o *ThresholdOptions) BackchannelOverrides() ThresholdOverride {
	o.mu.RLock()
	value := cloneThresholdOverride(o.backchannelOverrides)
	o.mu.RUnlock()
	return value
}

func (o *ThresholdOptions) Thresholds() map[string]float64 {
	o.mu.RLock()
	value := maps.Clone(o.thresholds)
	o.mu.RUnlock()
	return value
}

func (o *ThresholdOptions) DefaultThreshold() (float64, bool) {
	o.mu.RLock()
	if o.defaultT == nil {
		o.mu.RUnlock()
		return 0, false
	}
	value := *o.defaultT
	o.mu.RUnlock()
	return value, true
}

func (o *ThresholdOptions) Lookup(language agents.LanguageCode) (float64, bool) {
	o.mu.RLock()
	value, ok := o.lookupLocked(string(language))
	o.mu.RUnlock()
	return value, ok
}

func (o *ThresholdOptions) lookupLocked(language string) (float64, bool) {
	key := normalizeTurnLanguage(language)
	if value, ok := o.thresholds[key]; ok {
		return value, true
	}
	if o.defaultT == nil {
		return 0, false
	}
	return *o.defaultT, true
}

func (o *ThresholdOptions) LookupBackchannel(language agents.LanguageCode) (float64, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	key := normalizeTurnLanguage(string(language))
	value, ok := o.bc[key]
	if !ok && o.bcDefault != nil {
		value, ok = *o.bcDefault, true
	}
	return value, ok && value > 0
}

func (o *ThresholdOptions) Supports(language agents.LanguageCode) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	// A new cloud session reports support until calibrated defaults arrive, so
	// the first utterance is not skipped while session.created is in flight.
	if o.model == TurnDetectorModelV1 && o.serverThresholds == nil {
		return true
	}
	_, ok := o.lookupLocked(string(language))
	return ok
}

func (o *ThresholdOptions) UpdateOverrides(value ThresholdOverride) error {
	normalized, err := normalizeThresholdOverride(value)
	if err != nil {
		return err
	}
	o.mu.Lock()
	o.overrides = normalized
	o.resolveLocked()
	o.mu.Unlock()
	return nil
}

func (o *ThresholdOptions) UpdateBackchannelOverrides(value ThresholdOverride) error {
	normalized, err := normalizeThresholdOverride(value)
	if err != nil {
		return err
	}
	o.mu.Lock()
	o.backchannelOverrides = normalized
	o.resolveLocked()
	o.mu.Unlock()
	return nil
}

// UpdateServerDefaults adopts calibrated gateway values. A degenerate session
// is deliberately an error even with a caller override: it signals an
// incompatible or unhealthy gateway and triggers local fallback.
func (o *ThresholdOptions) UpdateServerDefaults(thresholds map[string]float32, fallback float32, backchannel map[string]float32, backchannelFallback float32) error {
	if len(thresholds) == 0 || fallback <= 0 || math.IsNaN(float64(fallback)) {
		return agents.NewAPIError("turn detector session created without usable default thresholds", nil, false, nil)
	}
	normalized := normalizeFloat32Thresholds(thresholds)
	bc := normalizeFloat32Thresholds(backchannel)
	defaultValue := roundThreshold(float64(fallback))

	o.mu.Lock()
	o.serverThresholds = normalized
	o.serverDefault = &defaultValue
	if len(bc) == 0 {
		o.serverBC = nil
	} else {
		o.serverBC = bc
	}
	if backchannelFallback > 0 {
		value := roundThreshold(float64(backchannelFallback))
		o.serverBCDefault = &value
	} else {
		o.serverBCDefault = nil
	}
	o.resolveLocked()
	o.mu.Unlock()
	return nil
}

func normalizeFloat32Thresholds(values map[string]float32) map[string]float64 {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]float64, len(values))
	for language, value := range values {
		result[normalizeTurnLanguage(language)] = roundThreshold(float64(value))
	}
	return result
}

func roundThreshold(value float64) float64 { return math.Round(value*1e4) / 1e4 }

// ToLocalFallback performs the one-way cloud-to-mini transition. When cloud
// defaults are known, effective thresholds are rescaled per language so a
// caller override keeps the same ratio to the model's calibration.
func (o *ThresholdOptions) ToLocalFallback() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.model == TurnDetectorModelV1Mini {
		return
	}
	var rescaled map[string]float64
	if o.serverThresholds != nil {
		rescaled = make(map[string]float64, len(o.serverThresholds))
		for language, serverValue := range o.serverThresholds {
			active, ok := o.lookupLocked(language)
			local, localOK := localTurnThreshold(language)
			if ok && localOK && serverValue != 0 {
				rescaled[language] = local * (active / serverValue)
			}
		}
	}
	o.model = TurnDetectorModelV1Mini
	o.serverThresholds = defaultLocalTurnThresholds()
	value, _ := localTurnThreshold("en")
	o.serverDefault = &value
	o.serverBC = nil
	o.serverBCDefault = nil
	o.resolveLocked()
	if rescaled != nil {
		resolvedDefault := o.defaultT
		o.thresholds = rescaled
		if english, ok := o.thresholds["en"]; ok {
			o.defaultT = float64Pointer(english)
		} else {
			o.defaultT = resolvedDefault
		}
	}
}

func (o *ThresholdOptions) resolveLocked() {
	o.thresholds, o.defaultT = resolveThresholdLayer(o.serverThresholds, o.serverDefault, o.overrides)
	o.bc, o.bcDefault = resolveThresholdLayer(o.serverBC, o.serverBCDefault, o.backchannelOverrides)
}

func resolveThresholdLayer(defaults map[string]float64, fallback *float64, override ThresholdOverride) (map[string]float64, *float64) {
	if defaults == nil || fallback == nil {
		if override.Scalar != nil {
			return map[string]float64{}, float64Pointer(*override.Scalar)
		}
		return map[string]float64{}, nil
	}
	if !override.set() {
		return maps.Clone(defaults), float64Pointer(*fallback)
	}
	if override.Scalar != nil {
		return map[string]float64{}, float64Pointer(*override.Scalar)
	}
	result := maps.Clone(defaults)
	maps.Copy(result, override.Languages)
	return result, float64Pointer(*fallback)
}

func float64Pointer(value float64) *float64 { return &value }

// ThresholdSnapshot is safe to serialize and never contains credentials or
// internal server state.
type ThresholdSnapshot struct {
	Model                TurnDetectorModel  `json:"model"`
	Thresholds           map[string]float64 `json:"thresholds"`
	DefaultThreshold     *float64           `json:"defaultThreshold,omitempty"`
	Overrides            ThresholdOverride  `json:"overrides,omitzero"`
	BackchannelOverrides ThresholdOverride  `json:"backchannelOverrides,omitzero"`
}

func (o *ThresholdOptions) Snapshot() ThresholdSnapshot {
	o.mu.RLock()
	result := ThresholdSnapshot{
		Model: o.model, Thresholds: maps.Clone(o.thresholds),
		Overrides:            cloneThresholdOverride(o.overrides),
		BackchannelOverrides: cloneThresholdOverride(o.backchannelOverrides),
	}
	if o.defaultT != nil {
		result.DefaultThreshold = float64Pointer(*o.defaultT)
	}
	o.mu.RUnlock()
	return result
}
