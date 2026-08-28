package paritymanifest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestCheckedInManifest(t *testing.T) {
	manifestPath, apiPath := checkedInPaths(t)
	summary, err := LoadAndValidate(manifestPath, apiPath)
	if err != nil {
		t.Fatalf("validate checked-in parity manifest: %v", err)
	}
	if summary.ExportsByLanguage["typescript"] != 1093 {
		t.Fatalf("TypeScript exports = %d, want 1093", summary.ExportsByLanguage["typescript"])
	}
	if summary.ExportsByLanguage["python"] != 472 {
		t.Fatalf("Python exports = %d, want 472", summary.ExportsByLanguage["python"])
	}
	if summary.EntriesByStatus["out_of_scope"] != 4 {
		t.Fatalf("out-of-scope records = %d, want 4", summary.EntriesByStatus["out_of_scope"])
	}
}

func TestValidationRejectsInvalidLedger(t *testing.T) {
	manifestData, apiData := checkedInData(t)

	tests := []struct {
		name string
		edit func(*Manifest)
		want string
	}{
		{
			name: "schema",
			edit: func(manifest *Manifest) { manifest.SchemaVersion++ },
			want: "schema_version",
		},
		{
			name: "typescript revision",
			edit: func(manifest *Manifest) { manifest.Upstreams.TypeScript.Revision = "moving-target" },
			want: "TypeScript revision",
		},
		{
			name: "python revision",
			edit: func(manifest *Manifest) { manifest.Upstreams.Python.Revision = "moving-target" },
			want: "python revision",
		},
		{
			name: "duplicate key",
			edit: func(manifest *Manifest) { manifest.Entries = append(manifest.Entries, manifest.Entries[0]) },
			want: "duplicate upstream key",
		},
		{
			name: "status",
			edit: func(manifest *Manifest) { manifest.Entries[0].Status = "mostly_done" },
			want: "unsupported status",
		},
		{
			name: "key identity",
			edit: func(manifest *Manifest) { manifest.Entries[0].Key += ".wrong" },
			want: "want",
		},
		{
			name: "missing package",
			edit: func(manifest *Manifest) {
				entry := firstMappedEntry(t, manifest)
				entry.Go.Package = GoModule + "/missing"
			},
			want: "absent from the API manifest",
		},
		{
			name: "missing symbol",
			edit: func(manifest *Manifest) {
				entry := firstMappedEntry(t, manifest)
				entry.Go.Symbol = "DefinitelyMissing"
			},
			want: "mapped Go declaration",
		},
		{
			name: "package-only non-namespace mapping",
			edit: func(manifest *Manifest) {
				entry := firstMappedEntry(t, manifest)
				entry.Go.Symbol = ""
			},
			want: "only a namespace",
		},
		{
			name: "idiom without equivalent",
			edit: func(manifest *Manifest) {
				entry := firstStatusEntry(t, manifest, "idiomatic_equivalent")
				entry.Go = nil
				entry.GoIdiom = ""
			},
			want: "requires a Go mapping or go_idiom",
		},
		{
			name: "python-only status on typescript",
			edit: func(manifest *Manifest) {
				entry := firstStatusEntry(t, manifest, "not_in_primary_js_baseline")
				entry.Upstream.Language = "typescript"
				entry.Key = "typescript:" + entry.Upstream.Namespace + "." + entry.Upstream.Name
			},
			want: "valid only for Python",
		},
		{
			name: "release blocker",
			edit: func(manifest *Manifest) {
				entry := firstStatusEntry(t, manifest, "implemented")
				entry.Status = "unimplemented"
				entry.Go = nil
				entry.Note = "Synthetic missing equivalent."
			},
			want: "unimplemented release blockers = 1",
		},
		{
			name: "summary drift",
			edit: func(manifest *Manifest) { manifest.StatusCounts["implemented"]++ },
			want: "status_counts",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var manifest Manifest
			if err := json.Unmarshal(manifestData, &manifest); err != nil {
				t.Fatal(err)
			}
			test.edit(&manifest)
			mutated, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			_, err = Validate(mutated, apiData)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestValidationToleratesUnmappedGoAdditions(t *testing.T) {
	manifestData, apiData := checkedInData(t)
	var api apiManifest
	if err := json.Unmarshal(apiData, &api); err != nil {
		t.Fatal(err)
	}
	api.Packages[0].Symbols = append(api.Packages[0].Symbols, apiSymbol{Name: "FutureGoAddition"})
	updatedAPI, err := json.Marshal(api)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Validate(manifestData, updatedAPI); err != nil {
		t.Fatalf("an unmapped Go API addition must be tolerated: %v", err)
	}
}

func TestValidationUsesStrictParitySchema(t *testing.T) {
	manifestData, apiData := checkedInData(t)
	var raw map[string]any
	if err := json.Unmarshal(manifestData, &raw); err != nil {
		t.Fatal(err)
	}
	raw["unexpected"] = true
	mutated, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Validate(mutated, apiData)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("Validate error = %v, want unknown-field error", err)
	}
}

func firstMappedEntry(t *testing.T, manifest *Manifest) *Entry {
	t.Helper()
	for index := range manifest.Entries {
		if manifest.Entries[index].Go != nil && manifest.Entries[index].Go.Symbol != "" {
			return &manifest.Entries[index]
		}
	}
	t.Fatal("manifest contains no mapped declarations")
	return nil
}

func firstStatusEntry(t *testing.T, manifest *Manifest, status string) *Entry {
	t.Helper()
	for index := range manifest.Entries {
		if manifest.Entries[index].Status == status {
			return &manifest.Entries[index]
		}
	}
	t.Fatalf("manifest contains no %q entry", status)
	return nil
}

func checkedInData(t *testing.T) ([]byte, []byte) {
	t.Helper()
	manifestPath, apiPath := checkedInPaths(t)
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	apiData, err := os.ReadFile(apiPath)
	if err != nil {
		t.Fatal(err)
	}
	return manifestData, apiData
}

func checkedInPaths(t *testing.T) (string, string) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	return filepath.Join(root, "docs", "parity-manifest.json"), filepath.Join(root, "docs", "api-manifest.json")
}
