// Command depaudit verifies the reviewed dependency and licensing policy for
// this module. It intentionally uses only the standard library so auditing the
// dependency graph does not add another dependency to that graph.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
)

const defaultPolicyPath = "internal/cmd/depaudit/policy.json"

type policy struct {
	SchemaVersion              int                 `json:"schema_version"`
	AllowedModuleLicenses      []string            `json:"allowed_module_licenses"`
	DisallowedModuleLicenses   []string            `json:"disallowed_module_licenses"`
	ReviewedDirectDependencies []reviewedDirect    `json:"reviewed_direct_dependencies"`
	ReviewedGraphExclusions    []reviewedExclusion `json:"reviewed_graph_exclusions"`
	SystemDependencies         []systemDependency  `json:"system_dependencies"`
	EmbeddedAssets             []embeddedAsset     `json:"embedded_assets"`
}

type reviewedDirect struct {
	Path         string         `json:"path"`
	Version      string         `json:"version"`
	License      string         `json:"license"`
	SourceURL    string         `json:"source_url"`
	LicenseFiles []reviewedFile `json:"license_files"`
}

type reviewedFile struct {
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
}

type reviewedExclusion struct {
	Path       string `json:"path"`
	Version    string `json:"version"`
	RequiredBy string `json:"required_by"`
	SourceURL  string `json:"source_url"`
	Reason     string `json:"reason"`
}

type systemDependency struct {
	Name              string `json:"name"`
	SPDX              string `json:"spdx"`
	SourceURL         string `json:"source_url"`
	DistributionNotes string `json:"distribution_notes"`
}

type embeddedAsset struct {
	Path           string `json:"path"`
	SHA256         string `json:"sha256"`
	SPDX           string `json:"spdx"`
	SourceURL      string `json:"source_url"`
	SourceRevision string `json:"source_revision"`
}

type module struct {
	Path     string  `json:"Path"`
	Version  string  `json:"Version"`
	Main     bool    `json:"Main"`
	Dir      string  `json:"Dir"`
	GoMod    string  `json:"GoMod"`
	Replace  *module `json:"Replace"`
	Indirect bool    `json:"Indirect,omitempty"`
}

type download struct {
	Path    string    `json:"Path"`
	Version string    `json:"Version"`
	Dir     string    `json:"Dir"`
	Error   string    `json:"Error"`
	Replace *download `json:"Replace"`
}

type editJSON struct {
	Require []moduleRequirement
	Replace []moduleReplacement
	Exclude []moduleReference
}

type moduleRequirement struct {
	Path     string
	Version  string
	Indirect bool
}

type moduleReplacement struct {
	Old moduleReference
	New moduleReference
}

type moduleReference struct {
	Path    string
	Version string
}

type fileEvidence struct {
	Name    string   `json:"name"`
	SHA256  string   `json:"sha256"`
	SPDXIDs []string `json:"spdx_ids,omitempty"`
}

type moduleInventory struct {
	Path              string         `json:"path"`
	Version           string         `json:"version"`
	Direct            bool           `json:"direct"`
	LicenseExpression string         `json:"license_expression,omitempty"`
	LicenseFiles      []fileEvidence `json:"license_files,omitempty"`
	NoticeFiles       []fileEvidence `json:"notice_files,omitempty"`
	SourceURL         string         `json:"source_url,omitempty"`
	ModuleURL         string         `json:"module_url"`
}

type inventory struct {
	SchemaVersion int                 `json:"schema_version"`
	Modules       []moduleInventory   `json:"modules"`
	LicenseCounts map[string]int      `json:"license_counts"`
	Exclusions    []reviewedExclusion `json:"reviewed_graph_exclusions,omitempty"`
	System        []systemDependency  `json:"system_dependencies"`
	Assets        []embeddedAsset     `json:"embedded_assets"`
	Problems      []string            `json:"problems,omitempty"`
}

type runner interface {
	Run(dir string, env []string, name string, args ...string) ([]byte, error)
}

type commandRunner struct{}

func (commandRunner) Run(dir string, env []string, name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return out, nil
}

func main() {
	root := flag.String("root", ".", "module root to audit")
	policyPath := flag.String("policy", defaultPolicyPath, "policy path, relative to the module root")
	format := flag.String("format", "text", "output format: text or json")
	flag.Parse()

	if *format != "text" && *format != "json" {
		fmt.Fprintf(os.Stderr, "depaudit: unsupported format %q\n", *format)
		os.Exit(2)
	}

	absRoot, err := filepath.Abs(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "depaudit: resolve root: %v\n", err)
		os.Exit(2)
	}
	absPolicy := *policyPath
	if !filepath.IsAbs(absPolicy) {
		absPolicy = filepath.Join(absRoot, absPolicy)
	}

	p, err := readPolicy(absPolicy)
	if err != nil {
		fmt.Fprintf(os.Stderr, "depaudit: %v\n", err)
		os.Exit(2)
	}
	inv, auditErr := audit(absRoot, p, commandRunner{})

	switch *format {
	case "json":
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(inv); err != nil {
			fmt.Fprintf(os.Stderr, "depaudit: encode inventory: %v\n", err)
			os.Exit(2)
		}
	case "text":
		writeText(os.Stdout, inv)
	}
	if auditErr != nil {
		fmt.Fprintf(os.Stderr, "depaudit: policy check failed: %v\n", auditErr)
		os.Exit(1)
	}
}

func readPolicy(path string) (policy, error) {
	f, err := os.Open(path)
	if err != nil {
		return policy{}, fmt.Errorf("open policy: %w", err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	var p policy
	if err := dec.Decode(&p); err != nil {
		return policy{}, fmt.Errorf("decode policy: %w", err)
	}
	if err := ensureJSONEOF(dec); err != nil {
		return policy{}, fmt.Errorf("decode policy: %w", err)
	}
	if err := validatePolicy(p); err != nil {
		return policy{}, err
	}
	return p, nil
}

func ensureJSONEOF(dec *json.Decoder) error {
	var extra any
	err := dec.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("unexpected data after JSON value")
	}
	return err
}

func validatePolicy(p policy) error {
	if p.SchemaVersion != 1 {
		return fmt.Errorf("unsupported policy schema version %d", p.SchemaVersion)
	}
	if len(p.AllowedModuleLicenses) == 0 {
		return errors.New("policy has no allowed module licenses")
	}
	if len(p.ReviewedDirectDependencies) == 0 {
		return errors.New("policy has no reviewed direct dependencies")
	}
	if duplicates(p.AllowedModuleLicenses) || duplicates(p.DisallowedModuleLicenses) {
		return errors.New("policy license lists contain duplicates")
	}
	allowed := toSet(p.AllowedModuleLicenses)
	disallowed := toSet(p.DisallowedModuleLicenses)
	for id := range allowed {
		if disallowed[id] {
			return fmt.Errorf("license %s is both allowed and disallowed", id)
		}
	}
	seenDirect := make(map[string]bool)
	for _, dep := range p.ReviewedDirectDependencies {
		if dep.Path == "" || dep.Version == "" || dep.License == "" || dep.SourceURL == "" || len(dep.LicenseFiles) == 0 {
			return fmt.Errorf("incomplete direct dependency policy for %q", dep.Path)
		}
		if !validSourceURL(dep.SourceURL) {
			return fmt.Errorf("direct dependency %s has invalid HTTPS source URL", dep.Path)
		}
		for _, id := range strings.Split(dep.License, " AND ") {
			if !allowed[id] {
				return fmt.Errorf("direct dependency %s uses unapproved license %s", dep.Path, id)
			}
		}
		if seenDirect[dep.Path] {
			return fmt.Errorf("duplicate direct dependency policy for %s", dep.Path)
		}
		seenDirect[dep.Path] = true
		if err := validateHashFiles(dep.LicenseFiles); err != nil {
			return fmt.Errorf("direct dependency %s: %w", dep.Path, err)
		}
	}
	seenExclusions := make(map[string]bool)
	for _, exclusion := range p.ReviewedGraphExclusions {
		key := moduleKey(exclusion.Path, exclusion.Version)
		if exclusion.Path == "" || exclusion.Version == "" || exclusion.RequiredBy == "" || exclusion.SourceURL == "" || exclusion.Reason == "" {
			return fmt.Errorf("incomplete graph exclusion policy for %q", key)
		}
		if !validSourceURL(exclusion.SourceURL) {
			return fmt.Errorf("graph exclusion %s has invalid HTTPS source URL", key)
		}
		if seenExclusions[key] {
			return fmt.Errorf("duplicate graph exclusion %s", key)
		}
		seenExclusions[key] = true
	}
	seenSystem := make(map[string]bool)
	for _, dep := range p.SystemDependencies {
		if dep.Name == "" || dep.SPDX == "" || dep.SourceURL == "" || dep.DistributionNotes == "" {
			return fmt.Errorf("incomplete system dependency policy for %q", dep.Name)
		}
		if !validSourceURL(dep.SourceURL) {
			return fmt.Errorf("system dependency %s has invalid HTTPS source URL", dep.Name)
		}
		if seenSystem[dep.Name] {
			return fmt.Errorf("duplicate system dependency %s", dep.Name)
		}
		seenSystem[dep.Name] = true
	}
	seenAssets := make(map[string]bool)
	for _, asset := range p.EmbeddedAssets {
		if asset.Path == "" || asset.SHA256 == "" || asset.SPDX == "" || asset.SourceURL == "" || asset.SourceRevision == "" {
			return fmt.Errorf("incomplete embedded asset policy for %q", asset.Path)
		}
		if seenAssets[asset.Path] {
			return fmt.Errorf("duplicate embedded asset %s", asset.Path)
		}
		seenAssets[asset.Path] = true
		if !validSourceURL(asset.SourceURL) {
			return fmt.Errorf("embedded asset %s has invalid HTTPS source URL", asset.Path)
		}
		if !allowed[asset.SPDX] {
			return fmt.Errorf("embedded asset %s uses unapproved license %s", asset.Path, asset.SPDX)
		}
		if !validSHA256(asset.SHA256) {
			return fmt.Errorf("embedded asset %s has invalid SHA-256", asset.Path)
		}
	}
	return nil
}

func validateHashFiles(files []reviewedFile) error {
	seen := make(map[string]bool)
	for _, f := range files {
		if f.Name == "" || !validSHA256(f.SHA256) {
			return fmt.Errorf("invalid reviewed file %q", f.Name)
		}
		if seen[f.Name] {
			return fmt.Errorf("duplicate reviewed file %s", f.Name)
		}
		seen[f.Name] = true
	}
	return nil
}

func validSHA256(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && strings.ToLower(s) == s
}

func validSourceURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil
}

func audit(root string, p policy, r runner) (inventory, error) {
	inv := inventory{
		SchemaVersion: 1,
		LicenseCounts: make(map[string]int),
		Exclusions:    append([]reviewedExclusion(nil), p.ReviewedGraphExclusions...),
		System:        append([]systemDependency(nil), p.SystemDependencies...),
		Assets:        append([]embeddedAsset(nil), p.EmbeddedAssets...),
	}
	problems := make([]string, 0)

	modfile, cleanup, err := stageModfile(root)
	if err != nil {
		return inv, err
	}
	defer cleanup()
	modfileFlag := "-modfile=" + modfile

	env := overrideEnv(os.Environ(), "GOWORK", "off")
	downloadOut, err := r.Run(root, env, "go", "mod", "download", "-json", modfileFlag, "all")
	if err != nil {
		return inv, err
	}
	var downloads []download
	if err := decodeJSONStream(downloadOut, &downloads); err != nil {
		return inv, fmt.Errorf("decode go mod download output: %w", err)
	}
	for _, d := range downloads {
		actual := d
		if d.Replace != nil {
			actual = *d.Replace
		}
		if actual.Error != "" {
			problems = append(problems, fmt.Sprintf("download %s@%s: %s", d.Path, d.Version, actual.Error))
		}
	}
	if _, err := r.Run(root, env, "go", "mod", "verify", modfileFlag); err != nil {
		return inv, err
	}

	listOut, err := r.Run(root, env, "go", "list", modfileFlag, "-mod=readonly", "-m", "-json", "all")
	if err != nil {
		return inv, err
	}
	var modules []module
	if err := decodeJSONStream(listOut, &modules); err != nil {
		return inv, fmt.Errorf("decode go list output: %w", err)
	}

	editOut, err := r.Run(root, env, "go", "mod", "edit", "-json", modfileFlag)
	if err != nil {
		return inv, err
	}
	var edit editJSON
	if err := json.Unmarshal(editOut, &edit); err != nil {
		return inv, fmt.Errorf("decode go mod edit output: %w", err)
	}
	direct := make(map[string]string)
	for _, req := range edit.Require {
		if !req.Indirect {
			direct[req.Path] = req.Version
		}
	}
	for _, replacement := range edit.Replace {
		old := replacement.Old.Path
		if replacement.Old.Version != "" {
			old += "@" + replacement.Old.Version
		}
		newPath := replacement.New.Path
		if replacement.New.Version != "" {
			newPath += "@" + replacement.New.Version
		}
		problems = append(problems, fmt.Sprintf("replace directive %s => %s requires dependency and license review", old, newPath))
	}
	problems = append(problems, compareExclusions(edit.Exclude, p.ReviewedGraphExclusions)...)

	depsOut, err := r.Run(root, env, "go", "list", modfileFlag, "-mod=readonly", "-deps", "-test", "-f", "{{.ImportPath}}", "./...")
	if err != nil {
		return inv, err
	}
	problems = append(problems, verifyExcludedPackagesUnreachable(depsOut, p.ReviewedGraphExclusions)...)

	allowed := toSet(p.AllowedModuleLicenses)
	disallowed := toSet(p.DisallowedModuleLicenses)
	reviewed := make(map[string]reviewedDirect, len(p.ReviewedDirectDependencies))
	for _, dep := range p.ReviewedDirectDependencies {
		reviewed[dep.Path] = dep
	}

	sort.Slice(modules, func(i, j int) bool {
		if modules[i].Path != modules[j].Path {
			return modules[i].Path < modules[j].Path
		}
		return modules[i].Version < modules[j].Version
	})
	for _, m := range modules {
		if m.Main {
			continue
		}
		actual := m
		if m.Replace != nil {
			actual = *m.Replace
		}
		mi := moduleInventory{
			Path:      m.Path,
			Version:   m.Version,
			Direct:    direct[m.Path] != "",
			ModuleURL: "https://pkg.go.dev/" + m.Path + "@" + m.Version,
		}
		if actual.Dir == "" {
			problems = append(problems, fmt.Sprintf("%s@%s: module directory is unavailable after download", m.Path, m.Version))
			inv.Modules = append(inv.Modules, mi)
			continue
		}
		licenseFiles, noticeFiles, scanErr := scanModule(actual.Dir)
		if scanErr != nil {
			problems = append(problems, fmt.Sprintf("%s@%s: %v", m.Path, m.Version, scanErr))
			inv.Modules = append(inv.Modules, mi)
			continue
		}
		mi.LicenseFiles = licenseFiles
		mi.NoticeFiles = noticeFiles
		ids := unionSPDX(licenseFiles)
		mi.LicenseExpression = strings.Join(ids, " AND ")
		if len(licenseFiles) == 0 {
			problems = append(problems, fmt.Sprintf("%s@%s: no root LICENSE, LICENCE, COPYING, or UNLICENSE file", m.Path, m.Version))
		} else {
			for _, evidence := range licenseFiles {
				if len(evidence.SPDXIDs) == 0 {
					problems = append(problems, fmt.Sprintf("%s@%s: %s has an unknown SPDX classification", m.Path, m.Version, evidence.Name))
				}
			}
			for _, id := range ids {
				inv.LicenseCounts[id]++
				switch {
				case disallowed[id]:
					problems = append(problems, fmt.Sprintf("%s@%s: disallowed license %s", m.Path, m.Version, id))
				case !allowed[id]:
					problems = append(problems, fmt.Sprintf("%s@%s: unreviewed license %s", m.Path, m.Version, id))
				}
			}
		}
		if mi.Direct {
			dep, ok := reviewed[m.Path]
			if !ok {
				problems = append(problems, fmt.Sprintf("%s@%s: direct dependency is not in reviewed policy", m.Path, m.Version))
			} else {
				mi.SourceURL = dep.SourceURL
				problems = append(problems, compareDirect(mi, dep)...)
			}
		}
		inv.Modules = append(inv.Modules, mi)
	}
	problems = append(problems, verifyExcludedModulesAbsent(inv.Modules, p.ReviewedGraphExclusions)...)

	for path, version := range direct {
		if _, ok := reviewed[path]; !ok {
			// The module loop reports this too when go list contains it. Keep this
			// guard for malformed/incomplete go list output and de-duplicate below.
			problems = append(problems, fmt.Sprintf("%s@%s: direct dependency is not in reviewed policy", path, version))
		}
	}
	for path, dep := range reviewed {
		version, ok := direct[path]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s@%s: reviewed direct dependency was removed; policy review required", path, dep.Version))
			continue
		}
		if version != dep.Version {
			problems = append(problems, fmt.Sprintf("%s: direct dependency changed from reviewed %s to %s", path, dep.Version, version))
		}
	}
	problems = append(problems, verifyAssets(root, p.EmbeddedAssets)...)

	sort.Slice(inv.System, func(i, j int) bool { return inv.System[i].Name < inv.System[j].Name })
	sort.Slice(inv.Assets, func(i, j int) bool { return inv.Assets[i].Path < inv.Assets[j].Path })
	sort.Slice(inv.Exclusions, func(i, j int) bool {
		return moduleKey(inv.Exclusions[i].Path, inv.Exclusions[i].Version) < moduleKey(inv.Exclusions[j].Path, inv.Exclusions[j].Version)
	})
	sort.Strings(problems)
	inv.Problems = compactStrings(problems)
	if len(inv.Problems) != 0 {
		return inv, fmt.Errorf("%d problem(s)", len(inv.Problems))
	}
	return inv, nil
}

func stageModfile(root string) (string, func(), error) {
	dir, err := os.MkdirTemp("", "livekit-agents-depaudit-")
	if err != nil {
		return "", nil, fmt.Errorf("create isolated module audit directory: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(dir) }

	modContents, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("read go.mod: %w", err)
	}
	modfile := filepath.Join(dir, "audit.mod")
	if err := os.WriteFile(modfile, modContents, 0o600); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("write isolated go.mod: %w", err)
	}

	sumContents, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		cleanup()
		return "", nil, fmt.Errorf("read go.sum: %w", err)
	}
	if err == nil {
		if err := os.WriteFile(filepath.Join(dir, "audit.sum"), sumContents, 0o600); err != nil {
			cleanup()
			return "", nil, fmt.Errorf("write isolated go.sum: %w", err)
		}
	}
	return modfile, cleanup, nil
}

func decodeJSONStream[T any](data []byte, dst *[]T) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	for {
		var value T
		err := dec.Decode(&value)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		*dst = append(*dst, value)
	}
}

func scanModule(dir string) ([]fileEvidence, []fileEvidence, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	var licenses, notices []fileEvidence
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		switch {
		case isLicenseFile(name):
			contents, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				return nil, nil, fmt.Errorf("read %s: %w", name, err)
			}
			licenses = append(licenses, fileEvidence{Name: name, SHA256: hashBytes(contents), SPDXIDs: classifyLicense(contents)})
		case isNoticeFile(name):
			contents, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				return nil, nil, fmt.Errorf("read %s: %w", name, err)
			}
			notices = append(notices, fileEvidence{Name: name, SHA256: hashBytes(contents)})
		}
	}
	sort.Slice(licenses, func(i, j int) bool { return licenses[i].Name < licenses[j].Name })
	sort.Slice(notices, func(i, j int) bool { return notices[i].Name < notices[j].Name })
	return licenses, notices, nil
}

func isLicenseFile(name string) bool {
	upper := strings.ToUpper(name)
	if strings.HasSuffix(upper, ".GO") {
		return false
	}
	return upper == "LICENSE" || upper == "LICENCE" || upper == "COPYING" || upper == "UNLICENSE" ||
		strings.HasPrefix(upper, "LICENSE.") || strings.HasPrefix(upper, "LICENSE-") ||
		strings.HasPrefix(upper, "LICENCE.") || strings.HasPrefix(upper, "LICENCE-") ||
		strings.HasPrefix(upper, "COPYING.") || strings.HasPrefix(upper, "COPYING-")
}

func isNoticeFile(name string) bool {
	upper := strings.ToUpper(name)
	if strings.HasSuffix(upper, ".GO") {
		return false
	}
	return upper == "NOTICE" || strings.HasPrefix(upper, "NOTICE.") || strings.HasPrefix(upper, "NOTICE-")
}

func classifyLicense(contents []byte) []string {
	// License templates are routinely reflowed. Collapsing whitespace keeps
	// classification stable across harmless line wrapping while the recorded
	// SHA-256 still makes every byte-level change to a reviewed direct license
	// require review.
	text := strings.ToLower(strings.Join(strings.Fields(string(contents)), " "))
	found := make(map[string]bool)
	contains := func(parts ...string) bool {
		for _, part := range parts {
			if !strings.Contains(text, part) {
				return false
			}
		}
		return true
	}

	isMPL := contains("mozilla public license", "version 2.0")
	if isMPL {
		found["MPL-2.0"] = true
	}
	// MPL-2.0 contains references to the GPL family as compatible secondary
	// licenses. Those references do not license the module under GPL, so only
	// run full-text GPL classification when this is not an MPL license.
	switch {
	case !isMPL && contains("gnu affero general public license", "version 3"):
		if contains("any later version") {
			found["AGPL-3.0-or-later"] = true
		} else {
			found["AGPL-3.0-only"] = true
		}
	case !isMPL && contains("gnu lesser general public license", "version 3"):
		if contains("any later version") {
			found["LGPL-3.0-or-later"] = true
		} else {
			found["LGPL-3.0-only"] = true
		}
	case !isMPL && contains("gnu lesser general public license", "version 2.1"):
		if contains("any later version") || contains("either version 2.1 of the license, or (at your option) any later version") {
			found["LGPL-2.1-or-later"] = true
		} else {
			found["LGPL-2.1-only"] = true
		}
	case !isMPL && contains("gnu general public license", "version 3"):
		if contains("any later version") {
			found["GPL-3.0-or-later"] = true
		} else {
			found["GPL-3.0-only"] = true
		}
	case !isMPL && contains("gnu general public license", "version 2"):
		if contains("any later version") {
			found["GPL-2.0-or-later"] = true
		} else {
			found["GPL-2.0-only"] = true
		}
	}

	if contains("apache license", "version 2.0") {
		found["Apache-2.0"] = true
	}
	if contains("cc0 1.0 universal") || contains("creative commons legal code", "cc0 1.0") {
		found["CC0-1.0"] = true
	}
	if contains("attribution-sharealike 4.0 international") {
		found["CC-BY-SA-4.0"] = true
	}
	if contains("this is free and unencumbered software released into the public domain") {
		found["Unlicense"] = true
	}
	if contains("business source license", "1.1") {
		found["BUSL-1.1"] = true
	}
	if contains("server side public license", "version 1") {
		found["SSPL-1.0"] = true
	}
	if contains("commons clause") {
		found["LicenseRef-Commons-Clause"] = true
	}
	if contains("permission to use, copy, modify, and/or distribute this software for any purpose with or without fee") {
		found["ISC"] = true
	}
	if contains("permission is hereby granted, free of charge, to any person obtaining a copy", "the software is provided \"as is\"") {
		found["MIT"] = true
	}
	if contains("redistribution and use in source and binary forms", "redistributions of source code must retain", "redistributions in binary form must reproduce", "disclaimer") {
		if strings.Contains(text, "neither the name") || strings.Contains(text, "nor the names of its contributors may be used to endorse") {
			found["BSD-3-Clause"] = true
		} else {
			found["BSD-2-Clause"] = true
		}
	}

	ids := make([]string, 0, len(found))
	for id := range found {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func unionSPDX(files []fileEvidence) []string {
	set := make(map[string]bool)
	for _, file := range files {
		for _, id := range file.SPDXIDs {
			set[id] = true
		}
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func compareDirect(actual moduleInventory, reviewed reviewedDirect) []string {
	var problems []string
	if actual.Version != reviewed.Version {
		problems = append(problems, fmt.Sprintf("%s: direct version %s does not match reviewed %s", actual.Path, actual.Version, reviewed.Version))
	}
	if actual.LicenseExpression != reviewed.License {
		problems = append(problems, fmt.Sprintf("%s@%s: license %q does not match reviewed %q", actual.Path, actual.Version, actual.LicenseExpression, reviewed.License))
	}
	actualFiles := make([]reviewedFile, 0, len(actual.LicenseFiles))
	for _, f := range actual.LicenseFiles {
		actualFiles = append(actualFiles, reviewedFile{Name: f.Name, SHA256: f.SHA256})
	}
	expectedFiles := append([]reviewedFile(nil), reviewed.LicenseFiles...)
	sort.Slice(actualFiles, func(i, j int) bool { return actualFiles[i].Name < actualFiles[j].Name })
	sort.Slice(expectedFiles, func(i, j int) bool { return expectedFiles[i].Name < expectedFiles[j].Name })
	if !slices.Equal(actualFiles, expectedFiles) {
		problems = append(problems, fmt.Sprintf("%s@%s: license file set or content changed; policy review required", actual.Path, actual.Version))
	}
	return problems
}

func compareExclusions(actual []moduleReference, reviewed []reviewedExclusion) []string {
	expected := make(map[string]bool, len(reviewed))
	for _, exclusion := range reviewed {
		expected[moduleKey(exclusion.Path, exclusion.Version)] = true
	}

	seen := make(map[string]bool, len(actual))
	var problems []string
	for _, exclusion := range actual {
		key := moduleKey(exclusion.Path, exclusion.Version)
		if seen[key] {
			problems = append(problems, fmt.Sprintf("duplicate exclude directive %s", key))
			continue
		}
		seen[key] = true
		if !expected[key] {
			problems = append(problems, fmt.Sprintf("exclude directive %s is not in reviewed policy", key))
		}
	}
	for key := range expected {
		if !seen[key] {
			problems = append(problems, fmt.Sprintf("reviewed graph exclusion %s is missing from go.mod", key))
		}
	}
	sort.Strings(problems)
	return problems
}

func verifyExcludedPackagesUnreachable(output []byte, exclusions []reviewedExclusion) []string {
	packages := strings.Fields(string(output))
	var problems []string
	for _, exclusion := range exclusions {
		for _, importPath := range packages {
			if importPath == exclusion.Path || strings.HasPrefix(importPath, exclusion.Path+"/") {
				problems = append(problems, fmt.Sprintf("excluded module %s is reachable through package %s", moduleKey(exclusion.Path, exclusion.Version), importPath))
			}
		}
	}
	sort.Strings(problems)
	return problems
}

func verifyExcludedModulesAbsent(modules []moduleInventory, exclusions []reviewedExclusion) []string {
	var problems []string
	for _, exclusion := range exclusions {
		for _, selected := range modules {
			if selected.Path == exclusion.Path {
				problems = append(problems, fmt.Sprintf("excluded module path %s is still selected at %s", exclusion.Path, selected.Version))
			}
		}
	}
	sort.Strings(problems)
	return problems
}

func moduleKey(path, version string) string {
	if version == "" {
		return path
	}
	return path + "@" + version
}

func verifyAssets(root string, assets []embeddedAsset) []string {
	var problems []string
	for _, asset := range assets {
		clean := filepath.Clean(asset.Path)
		if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			problems = append(problems, fmt.Sprintf("embedded asset path escapes module root: %s", asset.Path))
			continue
		}
		contents, err := os.ReadFile(filepath.Join(root, clean))
		if err != nil {
			problems = append(problems, fmt.Sprintf("embedded asset %s: %v", asset.Path, err))
			continue
		}
		if got := hashBytes(contents); got != asset.SHA256 {
			problems = append(problems, fmt.Sprintf("embedded asset %s: SHA-256 changed from reviewed %s to %s", asset.Path, asset.SHA256, got))
		}
	}
	return problems
}

func hashBytes(contents []byte) string {
	sum := sha256.Sum256(contents)
	return hex.EncodeToString(sum[:])
}

func writeText(w io.Writer, inv inventory) {
	direct := 0
	classified := 0
	for _, m := range inv.Modules {
		if m.Direct {
			direct++
		}
		if m.LicenseExpression != "" {
			classified++
		}
	}
	fmt.Fprintf(w, "Dependency graph: %d third-party modules (%d direct)\n", len(inv.Modules), direct)
	fmt.Fprintf(w, "License evidence: %d classified, %d unresolved\n", classified, len(inv.Modules)-classified)
	if len(inv.Exclusions) != 0 {
		fmt.Fprintf(w, "Reviewed graph exclusions: %d (must remain absent and unreachable)\n", len(inv.Exclusions))
	}
	ids := make([]string, 0, len(inv.LicenseCounts))
	for id := range inv.LicenseCounts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		fmt.Fprintf(w, "  %-22s %d\n", id, inv.LicenseCounts[id])
	}
	if len(inv.Problems) != 0 {
		fmt.Fprintln(w, "Problems:")
		for _, problem := range inv.Problems {
			fmt.Fprintf(w, "  - %s\n", problem)
		}
		return
	}
	fmt.Fprintln(w, "Dependency and license policy: PASS")
}

func toSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}
	return set
}

func duplicates(values []string) bool {
	return len(toSet(values)) != len(values)
}

func compactStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

func overrideEnv(env []string, key, value string) []string {
	prefix := key + "="
	out := make([]string, 0, len(env)+1)
	for _, entry := range env {
		if !strings.HasPrefix(entry, prefix) {
			out = append(out, entry)
		}
	}
	return append(out, prefix+value)
}
