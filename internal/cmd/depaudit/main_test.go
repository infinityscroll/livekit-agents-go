package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const (
	mitText = `MIT License
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software").
THE SOFTWARE IS PROVIDED "AS IS".`
	bsd2Text = `Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
Redistributions of source code must retain this copyright and disclaimer.
Redistributions in binary form must reproduce this copyright and disclaimer.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS".`
	bsd3Text = bsd2Text + ` Neither the name of Example nor the names of its
contributors may be used to endorse products derived from this software.`
)

func TestClassifyLicense(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		text string
		want []string
	}{
		{name: "apache", text: "Apache License\nVersion 2.0, January 2004", want: []string{"Apache-2.0"}},
		{name: "mit reflowed", text: mitText, want: []string{"MIT"}},
		{name: "bsd two clause", text: bsd2Text, want: []string{"BSD-2-Clause"}},
		{name: "bsd three clause", text: bsd3Text, want: []string{"BSD-3-Clause"}},
		{name: "isc reflowed", text: "Permission to use, copy, modify, and/or distribute this software for any\npurpose with or without fee is hereby granted.", want: []string{"ISC"}},
		{name: "mpl", text: "Mozilla Public License, version 2.0", want: []string{"MPL-2.0"}},
		{name: "mpl compatibility references are not gpl", text: "Mozilla Public License, version 2.0; GNU General Public License Version 3 or any later version", want: []string{"MPL-2.0"}},
		{name: "cc zero", text: "CC0 1.0 Universal", want: []string{"CC0-1.0"}},
		{name: "creative commons docs", text: "Attribution-ShareAlike 4.0 International", want: []string{"CC-BY-SA-4.0"}},
		{name: "unlicense", text: "This is free and unencumbered software released into the public domain.", want: []string{"Unlicense"}},
		{name: "business source", text: "Business Source License 1.1", want: []string{"BUSL-1.1"}},
		{name: "server side public", text: "Server Side Public License Version 1", want: []string{"SSPL-1.0"}},
		{name: "commons clause", text: "The Software is provided to you by the Licensor under the License, as defined below, subject to the following condition. Commons Clause", want: []string{"LicenseRef-Commons-Clause"}},
		{name: "gpl", text: "GNU GENERAL PUBLIC LICENSE Version 3; either version 3 or any later version", want: []string{"GPL-3.0-or-later"}},
		{name: "lgpl", text: "GNU LESSER GENERAL PUBLIC LICENSE Version 2.1; either version 2.1 of the License, or (at your option) any later version", want: []string{"LGPL-2.1-or-later"}},
		{name: "composite", text: "Apache License Version 2.0\n" + bsd3Text, want: []string{"Apache-2.0", "BSD-3-Clause"}},
		{name: "unknown", text: "Copyright Example. All rights reserved.", want: nil},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := classifyLicense([]byte(tt.text)); !slices.Equal(got, tt.want) {
				t.Fatalf("classifyLicense() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvidenceFileNames(t *testing.T) {
	t.Parallel()
	licenseTests := map[string]bool{
		"LICENSE":         true,
		"LICENCE":         true,
		"COPYING":         true,
		"LICENSE.txt":     true,
		"LICENSE.docs":    true,
		"LICENSE-libyaml": true,
		"licence.md":      true,
		"license_test.go": false,
		"UNLICENSE":       true,
		"README.md":       false,
	}
	for name, want := range licenseTests {
		if got := isLicenseFile(name); got != want {
			t.Errorf("isLicenseFile(%q) = %v, want %v", name, got, want)
		}
	}
	if !isNoticeFile("NOTICE") || !isNoticeFile("NOTICE.txt") || isNoticeFile("NOTICE.go") {
		t.Fatal("notice filename classification is incorrect")
	}
}

func TestOverrideEnvReplacesExistingValue(t *testing.T) {
	t.Parallel()
	got := overrideEnv([]string{"A=1", "GOWORK=workspace", "B=2", "GOWORK=other"}, "GOWORK", "off")
	if want := []string{"A=1", "B=2", "GOWORK=off"}; !slices.Equal(got, want) {
		t.Fatalf("overrideEnv() = %v, want %v", got, want)
	}
}

func TestStageModfileKeepsRepositoryFilesReadOnly(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	const modContents = "module example.com/main\n\ngo 1.26.0\n"
	const sumContents = "example.com/dependency v1.0.0/go.mod h1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n"
	mustWrite(t, filepath.Join(root, "go.mod"), modContents)
	mustWrite(t, filepath.Join(root, "go.sum"), sumContents)

	modfile, cleanup, err := stageModfile(root)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(modfile); err != nil || string(got) != modContents {
		t.Fatalf("staged go.mod = %q, %v", got, err)
	}
	stagedSum := strings.TrimSuffix(modfile, ".mod") + ".sum"
	if err := os.WriteFile(stagedSum, []byte("changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(root, "go.sum")); err != nil || string(got) != sumContents {
		t.Fatalf("repository go.sum changed: %q, %v", got, err)
	}
	cleanup()
	if _, err := os.Stat(modfile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staged modfile still exists after cleanup: %v", err)
	}
}

func TestScanModuleUsesActualRootEvidence(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mustWrite(t, filepath.Join(dir, "LICENCE"), mitText)
	mustWrite(t, filepath.Join(dir, "LICENSE.docs"), "Attribution-ShareAlike 4.0 International")
	mustWrite(t, filepath.Join(dir, "NOTICE"), "attribution")
	mustWrite(t, filepath.Join(dir, "license_test.go"), "not evidence")
	if err := os.Mkdir(filepath.Join(dir, "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, filepath.Join(dir, "nested", "LICENSE"), "must not be inferred for the whole module")

	licenses, notices, err := scanModule(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := evidenceNames(licenses), []string{"LICENCE", "LICENSE.docs"}; !slices.Equal(got, want) {
		t.Fatalf("license evidence = %v, want %v", got, want)
	}
	if got, want := unionSPDX(licenses), []string{"CC-BY-SA-4.0", "MIT"}; !slices.Equal(got, want) {
		t.Fatalf("SPDX union = %v, want %v", got, want)
	}
	if got := evidenceNames(notices); !slices.Equal(got, []string{"NOTICE"}) {
		t.Fatalf("notice evidence = %v", got)
	}
}

func TestCompareDirectRequiresExactVersionLicenseAndEvidence(t *testing.T) {
	t.Parallel()
	actual := moduleInventory{
		Path:              "example.com/direct",
		Version:           "v1.0.0",
		LicenseExpression: "MIT",
		LicenseFiles:      []fileEvidence{{Name: "LICENSE", SHA256: hashBytes([]byte(mitText))}},
	}
	reviewed := reviewedDirect{
		Path:      actual.Path,
		Version:   actual.Version,
		License:   actual.LicenseExpression,
		SourceURL: "https://example.com/direct",
		LicenseFiles: []reviewedFile{{
			Name:   "LICENSE",
			SHA256: actual.LicenseFiles[0].SHA256,
		}},
	}
	if got := compareDirect(actual, reviewed); len(got) != 0 {
		t.Fatalf("unchanged dependency produced problems: %v", got)
	}
	changed := actual
	changed.Version = "v1.1.0"
	changed.LicenseExpression = "BSD-3-Clause"
	changed.LicenseFiles = []fileEvidence{{Name: "LICENSE", SHA256: strings.Repeat("0", 64)}}
	got := strings.Join(compareDirect(changed, reviewed), "\n")
	for _, needle := range []string{"version", "license", "license file set or content changed"} {
		if !strings.Contains(got, needle) {
			t.Errorf("problems %q do not mention %q", got, needle)
		}
	}
}

func TestCompareExclusionsRequiresExactReviewedDirective(t *testing.T) {
	t.Parallel()
	reviewed := []reviewedExclusion{{Path: "example.com/excluded", Version: "v1.2.3"}}
	if got := compareExclusions([]moduleReference{{Path: "example.com/excluded", Version: "v1.2.3"}}, reviewed); len(got) != 0 {
		t.Fatalf("matching exclusion produced problems: %v", got)
	}

	got := strings.Join(compareExclusions([]moduleReference{{Path: "example.com/other", Version: "v2.0.0"}}, reviewed), "\n")
	for _, needle := range []string{
		"exclude directive example.com/other@v2.0.0 is not in reviewed policy",
		"reviewed graph exclusion example.com/excluded@v1.2.3 is missing from go.mod",
	} {
		if !strings.Contains(got, needle) {
			t.Errorf("problems do not include %q:\n%s", needle, got)
		}
	}
}

func TestExcludedModuleMustRemainAbsentAndUnreachable(t *testing.T) {
	t.Parallel()
	exclusions := []reviewedExclusion{{Path: "example.com/excluded", Version: "v1.2.3"}}
	if got := verifyExcludedPackagesUnreachable([]byte("example.com/allowed\n"), exclusions); len(got) != 0 {
		t.Fatalf("unrelated package produced reachability problems: %v", got)
	}
	if got := verifyExcludedModulesAbsent([]moduleInventory{{Path: "example.com/allowed", Version: "v1.0.0"}}, exclusions); len(got) != 0 {
		t.Fatalf("unrelated module produced graph problems: %v", got)
	}

	reachable := strings.Join(verifyExcludedPackagesUnreachable([]byte("example.com/excluded/codec\n"), exclusions), "\n")
	if !strings.Contains(reachable, "excluded module example.com/excluded@v1.2.3 is reachable through package example.com/excluded/codec") {
		t.Fatalf("reachability problems = %q", reachable)
	}
	selected := strings.Join(verifyExcludedModulesAbsent([]moduleInventory{{Path: "example.com/excluded", Version: "v9.9.9"}}, exclusions), "\n")
	if !strings.Contains(selected, "excluded module path example.com/excluded is still selected at v9.9.9") {
		t.Fatalf("graph problems = %q", selected)
	}
}

func TestVerifyAssetsRejectsTraversalAndChangedBytes(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "asset.ogg"), "reviewed")
	assets := []embeddedAsset{
		{Path: "asset.ogg", SHA256: hashBytes([]byte("different"))},
		{Path: "../escape.ogg", SHA256: strings.Repeat("0", 64)},
	}
	got := strings.Join(verifyAssets(root, assets), "\n")
	if !strings.Contains(got, "SHA-256 changed") || !strings.Contains(got, "escapes module root") {
		t.Fatalf("verifyAssets() = %q", got)
	}
}

func TestAuditDeterministicInventory(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "go.mod"), "module example.com/main\n\ngo 1.26.0\n")
	directDir := filepath.Join(root, "direct")
	indirectDir := filepath.Join(root, "indirect")
	for _, dir := range []string{directDir, indirectDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	mustWrite(t, filepath.Join(directDir, "LICENSE"), mitText)
	mustWrite(t, filepath.Join(indirectDir, "LICENSE"), bsd3Text)
	mustWrite(t, filepath.Join(root, "asset.ogg"), "asset")

	p := fixturePolicy(directDir)
	p.ReviewedGraphExclusions = []reviewedExclusion{{
		Path: "example.com/excluded", Version: "v1.2.3", RequiredBy: "example.com/indirect@v2.0.0",
		SourceURL: "https://example.com/excluded", Reason: "Unlicensed and unreachable test fixture.",
	}}
	p.EmbeddedAssets = []embeddedAsset{{
		Path: "asset.ogg", SHA256: hashBytes([]byte("asset")), SPDX: "Apache-2.0",
		SourceURL: "https://example.com/asset", SourceRevision: "deadbeef",
	}}
	r := fakeRunner{
		"go mod download -json all": jsonStream(t,
			download{Path: "example.com/direct", Version: "v1.0.0", Dir: directDir},
			download{Path: "example.com/indirect", Version: "v2.0.0", Dir: indirectDir},
		),
		"go list -mod=readonly -m -json all": jsonStream(t,
			module{Path: "example.com/main", Main: true, Dir: root},
			// Deliberately reverse lexical order to prove stable output.
			module{Path: "example.com/indirect", Version: "v2.0.0", Dir: indirectDir},
			module{Path: "example.com/direct", Version: "v1.0.0", Dir: directDir},
		),
		"go mod verify": {},
		"go list -mod=readonly -deps -test -f {{.ImportPath}} ./...": []byte("example.com/main\n"),
		"go mod edit -json": mustJSON(t, editJSON{
			Require: []moduleRequirement{
				{Path: "example.com/direct", Version: "v1.0.0"},
				{Path: "example.com/indirect", Version: "v2.0.0", Indirect: true},
			},
			Exclude: []moduleReference{{Path: "example.com/excluded", Version: "v1.2.3"}},
		}),
	}

	first, err := audit(root, p, r)
	if err != nil {
		t.Fatalf("audit() error = %v, problems = %v", err, first.Problems)
	}
	second, err := audit(root, p, r)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON := mustJSON(t, first)
	secondJSON := mustJSON(t, second)
	if !bytes.Equal(firstJSON, secondJSON) {
		t.Fatalf("inventory is nondeterministic:\n%s\n%s", firstJSON, secondJSON)
	}
	if got := []string{first.Modules[0].Path, first.Modules[1].Path}; !slices.Equal(got, []string{"example.com/direct", "example.com/indirect"}) {
		t.Fatalf("module order = %v", got)
	}
	if first.Modules[0].SourceURL != "https://example.com/direct" || !first.Modules[0].Direct || first.Modules[1].Direct {
		t.Fatalf("direct metadata is incorrect: %+v", first.Modules)
	}
	if strings.Contains(string(firstJSON), directDir) || strings.Contains(string(firstJSON), indirectDir) {
		t.Fatalf("inventory leaks host-specific module directories: %s", firstJSON)
	}

	var text bytes.Buffer
	writeText(&text, first)
	if !strings.Contains(text.String(), "Dependency and license policy: PASS") {
		t.Fatalf("text output = %q", text.String())
	}
	if !strings.Contains(text.String(), "Reviewed graph exclusions: 1") {
		t.Fatalf("text output does not report reviewed exclusion: %q", text.String())
	}

	r["go list -mod=readonly -deps -test -f {{.ImportPath}} ./..."] = []byte("example.com/excluded/codec\n")
	failed, err := audit(root, p, r)
	if err == nil {
		t.Fatal("audit unexpectedly accepted a reachable excluded package")
	}
	if got := strings.Join(failed.Problems, "\n"); !strings.Contains(got, "excluded module example.com/excluded@v1.2.3 is reachable") {
		t.Fatalf("reachability problem not reported: %s", got)
	}
}

func TestAuditFailsMissingUnknownDisallowedAndUnreviewedDirect(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "go.mod"), "module example.com/main\n\ngo 1.26.0\n")
	dirs := map[string]string{}
	for _, name := range []string{"reviewed", "added", "unknown", "gpl", "missing"} {
		dir := filepath.Join(root, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		dirs[name] = dir
	}
	mustWrite(t, filepath.Join(dirs["reviewed"], "LICENSE"), mitText)
	mustWrite(t, filepath.Join(dirs["added"], "LICENSE"), mitText)
	mustWrite(t, filepath.Join(dirs["unknown"], "LICENSE"), "custom terms")
	mustWrite(t, filepath.Join(dirs["gpl"], "COPYING"), "GNU GENERAL PUBLIC LICENSE Version 3, or any later version")

	p := fixturePolicy(dirs["reviewed"])
	p.ReviewedDirectDependencies[0].Path = "example.com/reviewed"
	p.ReviewedDirectDependencies[0].SourceURL = "https://example.com/reviewed"
	p.ReviewedDirectDependencies = append(p.ReviewedDirectDependencies, reviewedDirect{
		Path: "example.com/removed", Version: "v1.0.0", License: "MIT", SourceURL: "https://example.com/removed",
		LicenseFiles: []reviewedFile{{Name: "LICENSE", SHA256: hashBytes([]byte(mitText))}},
	})
	p.DisallowedModuleLicenses = []string{"GPL-3.0-or-later"}
	mods := []module{{Path: "example.com/main", Main: true, Dir: root}}
	downloads := make([]download, 0)
	for _, name := range []string{"reviewed", "added", "unknown", "gpl", "missing"} {
		path := "example.com/" + name
		mods = append(mods, module{Path: path, Version: "v1.0.0", Dir: dirs[name]})
		downloads = append(downloads, download{Path: path, Version: "v1.0.0", Dir: dirs[name]})
	}
	r := fakeRunner{
		"go mod download -json all":                                  jsonStreamSlice(t, downloads),
		"go mod verify":                                              {},
		"go list -mod=readonly -m -json all":                         jsonStreamSlice(t, mods),
		"go list -mod=readonly -deps -test -f {{.ImportPath}} ./...": []byte("example.com/main\n"),
		"go mod edit -json": mustJSON(t, editJSON{
			Require: []moduleRequirement{
				{Path: "example.com/reviewed", Version: "v1.0.0"},
				{Path: "example.com/added", Version: "v1.0.0"},
			},
			Replace: []moduleReplacement{{
				Old: moduleReference{Path: "example.com/unknown", Version: "v1.0.0"},
				New: moduleReference{Path: "../local-unknown"},
			}},
		}),
	}

	inv, err := audit(root, p, r)
	if err == nil {
		t.Fatal("audit unexpectedly passed")
	}
	got := strings.Join(inv.Problems, "\n")
	for _, needle := range []string{
		"example.com/added@v1.0.0: direct dependency is not in reviewed policy",
		"example.com/unknown@v1.0.0: LICENSE has an unknown SPDX classification",
		"example.com/gpl@v1.0.0: disallowed license GPL-3.0-or-later",
		"example.com/missing@v1.0.0: no root LICENSE",
		"example.com/removed@v1.0.0: reviewed direct dependency was removed",
		"replace directive example.com/unknown@v1.0.0 => ../local-unknown requires dependency and license review",
	} {
		if !strings.Contains(got, needle) {
			t.Errorf("problems do not include %q:\n%s", needle, got)
		}
	}
}

func TestReadPolicyRejectsUnknownFieldsAndTrailingJSON(t *testing.T) {
	t.Parallel()
	for name, body := range map[string]string{
		"unknown":  `{"schema_version":1,"unknown":true}`,
		"trailing": `{"schema_version":1} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "policy.json")
			mustWrite(t, path, body)
			if _, err := readPolicy(path); err == nil {
				t.Fatal("readPolicy unexpectedly succeeded")
			}
		})
	}
}

func TestRepositoryPolicyIsWellFormed(t *testing.T) {
	t.Parallel()
	p, err := readPolicy("policy.json")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(p.ReviewedDirectDependencies), 14; got != want {
		t.Fatalf("reviewed direct dependency count = %d, want %d", got, want)
	}
	if got, want := len(p.ReviewedGraphExclusions), 1; got != want {
		t.Fatalf("reviewed graph exclusion count = %d, want %d", got, want)
	}
	if got, want := len(p.SystemDependencies), 4; got != want {
		t.Fatalf("system dependency count = %d, want %d", got, want)
	}
	if got, want := len(p.EmbeddedAssets), 4; got != want {
		t.Fatalf("embedded asset count = %d, want %d", got, want)
	}
}

func fixturePolicy(directDir string) policy {
	license, _, err := scanModule(directDir)
	if err != nil {
		panic(err)
	}
	return policy{
		SchemaVersion:            1,
		AllowedModuleLicenses:    []string{"BSD-3-Clause", "MIT"},
		DisallowedModuleLicenses: []string{"GPL-3.0-or-later"},
		ReviewedDirectDependencies: []reviewedDirect{{
			Path: "example.com/direct", Version: "v1.0.0", License: "MIT", SourceURL: "https://example.com/direct",
			LicenseFiles: []reviewedFile{{Name: "LICENSE", SHA256: license[0].SHA256}},
		}},
	}
}

type fakeRunner map[string][]byte

func (f fakeRunner) Run(_ string, _ []string, name string, args ...string) ([]byte, error) {
	filtered := make([]string, 0, len(args)+1)
	filtered = append(filtered, name)
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-modfile=") {
			filtered = append(filtered, arg)
		}
	}
	key := strings.Join(filtered, " ")
	out, ok := f[key]
	if !ok {
		return nil, errors.New("unexpected command: " + key)
	}
	return append([]byte(nil), out...), nil
}

func jsonStream(t *testing.T, values ...any) []byte {
	t.Helper()
	var out []byte
	for _, value := range values {
		out = append(out, mustJSON(t, value)...)
		out = append(out, '\n')
	}
	return out
}

func jsonStreamSlice[T any](t *testing.T, values []T) []byte {
	t.Helper()
	var out []byte
	for _, value := range values {
		out = append(out, mustJSON(t, value)...)
		out = append(out, '\n')
	}
	return out
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustWrite(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func evidenceNames(files []fileEvidence) []string {
	result := make([]string, len(files))
	for i, file := range files {
		result[i] = file.Name
	}
	return result
}
