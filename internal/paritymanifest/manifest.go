// Package paritymanifest validates the checked-in cross-language API parity ledger.
package paritymanifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

const (
	SchemaVersion               = 1
	TypeScriptRevision          = "128f3f6a230616e960325b112067861ce1f1a17f"
	PythonRevision              = "cdb37ade6f8e80822e6c5ec4e6de457f2dcaf637"
	GoModule                    = "github.com/livekit/agents-go"
	GoAPIManifest               = "docs/api-manifest.json"
	TypeScriptExports           = 1093
	PythonExports               = 472
	TypeScriptElevenLabsExports = 17
	PythonElevenLabsExports     = 11
	ScopeRecords                = 4
)

var allowedStatuses = map[string]struct{}{
	"implemented":                 {},
	"idiomatic_equivalent":        {},
	"unavailable_upstream_go_rtc": {},
	"out_of_scope":                {},
	"not_in_primary_js_baseline":  {},
	"unimplemented":               {},
}

// Manifest is the on-disk parity ledger schema.
type Manifest struct {
	SchemaVersion int            `json:"schema_version"`
	Upstreams     Upstreams      `json:"upstreams"`
	Enumeration   Enumeration    `json:"enumeration"`
	ExportCounts  map[string]int `json:"export_counts"`
	StatusCounts  map[string]int `json:"status_counts"`
	Entries       []Entry        `json:"entries"`
}

type Upstreams struct {
	TypeScript SourceRevision `json:"typescript"`
	Python     SourceRevision `json:"python"`
	Go         GoSource       `json:"go"`
}

type SourceRevision struct {
	Repository        string `json:"repository"`
	Revision          string `json:"revision"`
	PrimaryEntrypoint string `json:"primary_entrypoint"`
	APIReport         string `json:"api_report,omitempty"`
}

type GoSource struct {
	Module                   string `json:"module"`
	APIManifest              string `json:"api_manifest"`
	APIManifestSchemaVersion int    `json:"api_manifest_schema_version"`
}

type Enumeration struct {
	TypeScript    string `json:"typescript"`
	Python        string `json:"python"`
	ExclusionRule string `json:"exclusion_rule"`
}

type Entry struct {
	Key      string     `json:"key"`
	Upstream Export     `json:"upstream"`
	Status   string     `json:"status"`
	Go       *GoMapping `json:"go,omitempty"`
	GoIdiom  string     `json:"go_idiom,omitempty"`
	Note     string     `json:"note,omitempty"`
}

type Export struct {
	Language   string `json:"language"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	SourcePath string `json:"source_path"`
}

type GoMapping struct {
	Package string `json:"package"`
	Symbol  string `json:"symbol,omitempty"`
}

// Summary is returned only after the complete manifest validates.
type Summary struct {
	ExportsByLanguage map[string]int
	EntriesByStatus   map[string]int
}

type apiManifest struct {
	SchemaVersion int          `json:"schema_version"`
	Module        string       `json:"module"`
	Packages      []apiPackage `json:"packages"`
}

type apiPackage struct {
	ImportPath string      `json:"import_path"`
	Symbols    []apiSymbol `json:"symbols"`
}

type apiSymbol struct {
	Name string `json:"name"`
}

// LoadAndValidate reads a parity manifest and the Go API manifest and validates
// their one-way relationship. New Go declarations do not require parity rows;
// every Go declaration referenced by a parity row must already exist.
func LoadAndValidate(manifestPath, apiManifestPath string) (Summary, error) {
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		return Summary{}, fmt.Errorf("read parity manifest: %w", err)
	}
	apiData, err := os.ReadFile(apiManifestPath)
	if err != nil {
		return Summary{}, fmt.Errorf("read API manifest: %w", err)
	}
	return Validate(manifestData, apiData)
}

// Validate checks schema invariants, pinned revisions, unique upstream keys,
// status contracts, summary counts, and referenced Go declarations.
func Validate(manifestData, apiManifestData []byte) (Summary, error) {
	var manifest Manifest
	if err := decodeStrict(manifestData, &manifest); err != nil {
		return Summary{}, fmt.Errorf("decode parity manifest: %w", err)
	}

	var api apiManifest
	if err := json.Unmarshal(apiManifestData, &api); err != nil {
		return Summary{}, fmt.Errorf("decode API manifest: %w", err)
	}
	if err := validateHeaders(manifest, api); err != nil {
		return Summary{}, err
	}

	declarations := make(map[string]map[string]struct{}, len(api.Packages))
	for _, pkg := range api.Packages {
		if pkg.ImportPath == "" {
			return Summary{}, errors.New("API manifest contains an empty import_path")
		}
		if _, exists := declarations[pkg.ImportPath]; exists {
			return Summary{}, fmt.Errorf("API manifest contains duplicate package %q", pkg.ImportPath)
		}
		symbols := make(map[string]struct{}, len(pkg.Symbols))
		for _, symbol := range pkg.Symbols {
			if symbol.Name == "" {
				return Summary{}, fmt.Errorf("API package %q contains an empty symbol", pkg.ImportPath)
			}
			symbols[symbol.Name] = struct{}{}
		}
		declarations[pkg.ImportPath] = symbols
	}

	exports := map[string]int{"typescript": 0, "python": 0}
	elevenLabs := map[string]int{"typescript": 0, "python": 0}
	statuses := make(map[string]int, len(allowedStatuses))
	seen := make(map[string]struct{}, len(manifest.Entries))
	previousKey := ""
	for index, entry := range manifest.Entries {
		if err := validateEntry(entry, declarations); err != nil {
			return Summary{}, fmt.Errorf("entry %d (%q): %w", index, entry.Key, err)
		}
		if _, exists := seen[entry.Key]; exists {
			return Summary{}, fmt.Errorf("duplicate upstream key %q", entry.Key)
		}
		seen[entry.Key] = struct{}{}
		if previousKey != "" && entry.Key <= previousKey {
			return Summary{}, fmt.Errorf("entries are not strictly sorted: %q follows %q", entry.Key, previousKey)
		}
		previousKey = entry.Key
		statuses[entry.Status]++
		if entry.Upstream.Kind != "scope" {
			exports[entry.Upstream.Language]++
			if entry.Upstream.Namespace == "plugins.elevenlabs" {
				elevenLabs[entry.Upstream.Language]++
			}
		}
	}
	if exports["typescript"] != TypeScriptExports || exports["python"] != PythonExports {
		return Summary{}, fmt.Errorf("audited export counts = typescript:%d python:%d, want typescript:%d python:%d", exports["typescript"], exports["python"], TypeScriptExports, PythonExports)
	}
	if elevenLabs["typescript"] != TypeScriptElevenLabsExports || elevenLabs["python"] != PythonElevenLabsExports {
		return Summary{}, fmt.Errorf("ElevenLabs export counts = typescript:%d python:%d, want typescript:%d python:%d", elevenLabs["typescript"], elevenLabs["python"], TypeScriptElevenLabsExports, PythonElevenLabsExports)
	}
	if statuses["out_of_scope"] != ScopeRecords {
		return Summary{}, fmt.Errorf("out_of_scope records = %d, want %d", statuses["out_of_scope"], ScopeRecords)
	}
	if blockers := statuses["unimplemented"]; blockers != 0 {
		return Summary{}, fmt.Errorf("unimplemented release blockers = %d, want 0", blockers)
	}
	for language := range manifest.ExportCounts {
		if language != "typescript" && language != "python" {
			return Summary{}, fmt.Errorf("export_counts contains unsupported language %q", language)
		}
	}
	for status := range manifest.StatusCounts {
		if _, ok := allowedStatuses[status]; !ok {
			return Summary{}, fmt.Errorf("status_counts contains unsupported status %q", status)
		}
	}

	if err := equalCounts("export_counts", manifest.ExportCounts, exports); err != nil {
		return Summary{}, err
	}
	if err := equalCounts("status_counts", manifest.StatusCounts, statuses); err != nil {
		return Summary{}, err
	}
	return Summary{ExportsByLanguage: exports, EntriesByStatus: statuses}, nil
}

func decodeStrict(data []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func validateHeaders(manifest Manifest, api apiManifest) error {
	if manifest.SchemaVersion != SchemaVersion {
		return fmt.Errorf("schema_version = %d, want %d", manifest.SchemaVersion, SchemaVersion)
	}
	if manifest.Upstreams.TypeScript.Revision != TypeScriptRevision {
		return fmt.Errorf("TypeScript revision = %q, want %q", manifest.Upstreams.TypeScript.Revision, TypeScriptRevision)
	}
	if manifest.Upstreams.Python.Revision != PythonRevision {
		return fmt.Errorf("python revision = %q, want %q", manifest.Upstreams.Python.Revision, PythonRevision)
	}
	if manifest.Upstreams.Go.Module != GoModule || api.Module != GoModule {
		return fmt.Errorf("go module mismatch: ledger=%q API=%q want=%q", manifest.Upstreams.Go.Module, api.Module, GoModule)
	}
	if manifest.Upstreams.Go.APIManifest != GoAPIManifest {
		return fmt.Errorf("go api_manifest = %q, want %q", manifest.Upstreams.Go.APIManifest, GoAPIManifest)
	}
	if api.SchemaVersion <= 0 || manifest.Upstreams.Go.APIManifestSchemaVersion != api.SchemaVersion {
		return fmt.Errorf("go API schema mismatch: ledger=%d API=%d", manifest.Upstreams.Go.APIManifestSchemaVersion, api.SchemaVersion)
	}
	if strings.TrimSpace(manifest.Enumeration.TypeScript) == "" ||
		strings.TrimSpace(manifest.Enumeration.Python) == "" ||
		strings.TrimSpace(manifest.Enumeration.ExclusionRule) == "" {
		return errors.New("enumeration rules must be non-empty")
	}
	if len(manifest.Entries) == 0 {
		return errors.New("entries must be non-empty")
	}
	return nil
}

func validateEntry(entry Entry, declarations map[string]map[string]struct{}) error {
	if entry.Upstream.Language != "typescript" && entry.Upstream.Language != "python" {
		return fmt.Errorf("unsupported upstream language %q", entry.Upstream.Language)
	}
	if entry.Upstream.Namespace == "" || entry.Upstream.Name == "" || entry.Upstream.Kind == "" || entry.Upstream.SourcePath == "" {
		return errors.New("upstream namespace, name, kind, and source_path are required")
	}
	wantKey := entry.Upstream.Language + ":" + entry.Upstream.Namespace + "." + entry.Upstream.Name
	if entry.Key != wantKey {
		return fmt.Errorf("key = %q, want %q", entry.Key, wantKey)
	}
	if _, ok := allowedStatuses[entry.Status]; !ok {
		return fmt.Errorf("unsupported status %q", entry.Status)
	}

	if entry.Go != nil {
		symbols, ok := declarations[entry.Go.Package]
		if !ok {
			return fmt.Errorf("mapped Go package %q is absent from the API manifest", entry.Go.Package)
		}
		if entry.Go.Symbol == "" {
			if entry.Upstream.Kind != "namespace" {
				return errors.New("only a namespace may map to a Go package without a symbol")
			}
		} else if _, ok := symbols[entry.Go.Symbol]; !ok {
			return fmt.Errorf("mapped Go declaration %s.%s is absent from the API manifest", entry.Go.Package, entry.Go.Symbol)
		}
	}

	switch entry.Status {
	case "implemented":
		if entry.Go == nil || entry.Go.Symbol == "" {
			return errors.New("implemented entries require a mapped Go declaration")
		}
		if entry.GoIdiom != "" {
			return errors.New("implemented entries cannot use go_idiom")
		}
	case "idiomatic_equivalent":
		if entry.Go == nil && strings.TrimSpace(entry.GoIdiom) == "" {
			return errors.New("idiomatic_equivalent requires a Go mapping or go_idiom")
		}
		if entry.GoIdiom != "" && strings.TrimSpace(entry.Note) == "" {
			return errors.New("go_idiom requires an explanatory note")
		}
	case "not_in_primary_js_baseline":
		if entry.Upstream.Language != "python" {
			return errors.New("not_in_primary_js_baseline is valid only for Python")
		}
		if entry.Go != nil || entry.GoIdiom != "" || strings.TrimSpace(entry.Note) == "" {
			return errors.New("not_in_primary_js_baseline requires a note and no Go mapping")
		}
	case "out_of_scope":
		if entry.Upstream.Kind != "scope" || entry.Go != nil || entry.GoIdiom != "" {
			return errors.New("out_of_scope is reserved for unmapped scope records")
		}
		if !strings.Contains(entry.Upstream.SourcePath, "examples") && !strings.Contains(entry.Upstream.SourcePath, "plugins") {
			return errors.New("out_of_scope source must be examples or plugins")
		}
		if strings.Contains(entry.Upstream.SourcePath, "elevenlabs/**") && !strings.Contains(entry.Upstream.SourcePath, "except") {
			return errors.New("ElevenLabs cannot be excluded")
		}
	case "unavailable_upstream_go_rtc", "unimplemented":
		if entry.Go != nil || entry.GoIdiom != "" || strings.TrimSpace(entry.Note) == "" {
			return fmt.Errorf("%s requires a note and no Go mapping", entry.Status)
		}
	}
	if entry.Upstream.Kind == "scope" && entry.Status != "out_of_scope" {
		return errors.New("scope records must be out_of_scope")
	}
	return nil
}

func equalCounts(label string, got, want map[string]int) error {
	keys := make(map[string]struct{}, len(got)+len(want))
	for key := range got {
		keys[key] = struct{}{}
	}
	for key := range want {
		keys[key] = struct{}{}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	for _, key := range ordered {
		if got[key] != want[key] {
			return fmt.Errorf("%s[%q] = %d, want %d", label, key, got[key], want[key])
		}
	}
	return nil
}
