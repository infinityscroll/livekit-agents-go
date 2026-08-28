// SPDX-License-Identifier: Apache-2.0

// Package apimanifest produces the deterministic public-API snapshot used by
// release checks. It deliberately uses only the standard library so the SDK's
// release tooling cannot pull dependencies into user binaries.
package apimanifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/scanner"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const SchemaVersion = 1

type Manifest struct {
	SchemaVersion int       `json:"schema_version"`
	Module        string    `json:"module"`
	Packages      []Package `json:"packages"`
}

type Package struct {
	ImportPath string   `json:"import_path"`
	Name       string   `json:"name"`
	Symbols    []Symbol `json:"symbols"`
}

type Symbol struct {
	Kind        string `json:"kind"`
	Name        string `json:"name"`
	Declaration string `json:"declaration"`
}

// Generate walks all public packages rooted at root. Internal tooling,
// benchmarks, test files, and command-only packages are excluded. Files for
// every build tag are inspected so platform-specific public API drift is
// caught before release.
func Generate(root string) (Manifest, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return Manifest{}, err
	}
	module, err := readModulePath(filepath.Join(root, "go.mod"))
	if err != nil {
		return Manifest{}, err
	}

	manifest := Manifest{SchemaVersion: SchemaVersion, Module: module}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if relative != "." && skipDirectory(relative) {
			return filepath.SkipDir
		}
		pkg, ok, err := inspectPackage(root, path, module)
		if err != nil {
			return err
		}
		if ok {
			manifest.Packages = append(manifest.Packages, pkg)
		}
		return nil
	})
	if err != nil {
		return Manifest{}, err
	}
	sort.Slice(manifest.Packages, func(i, j int) bool {
		return manifest.Packages[i].ImportPath < manifest.Packages[j].ImportPath
	})
	return manifest, nil
}

func Marshal(manifest Manifest) ([]byte, error) {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func readModulePath(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read module file: %w", err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "module" {
			return fields[1], nil
		}
	}
	return "", errors.New("module path is missing from go.mod")
}

func skipDirectory(relative string) bool {
	for _, part := range strings.Split(filepath.ToSlash(relative), "/") {
		if part == "internal" || part == "benchmarks" || part == "vendor" || strings.HasPrefix(part, ".") {
			return true
		}
	}
	return false
}

func inspectPackage(root, directory, module string) (Package, bool, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return Package{}, false, err
	}
	fset := token.NewFileSet()
	files := make([]*ast.File, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(directory, name), nil, parser.SkipObjectResolution)
		if err != nil {
			return Package{}, false, fmt.Errorf("parse %s: %w", filepath.Join(directory, name), err)
		}
		files = append(files, file)
	}
	if len(files) == 0 {
		return Package{}, false, nil
	}
	packageName := files[0].Name.Name
	if packageName == "main" {
		return Package{}, false, nil
	}
	for _, file := range files[1:] {
		if file.Name.Name != packageName {
			return Package{}, false, fmt.Errorf("mixed packages %q and %q in %s", packageName, file.Name.Name, directory)
		}
	}
	relative, err := filepath.Rel(root, directory)
	if err != nil {
		return Package{}, false, err
	}
	importPath := module
	if relative != "." {
		importPath += "/" + filepath.ToSlash(relative)
	}
	pkg := Package{ImportPath: importPath, Name: packageName}
	seen := make(map[string]Symbol)
	for _, file := range files {
		for _, declaration := range file.Decls {
			for _, symbol := range exportedSymbols(fset, declaration) {
				key := symbol.Kind + "\x00" + symbol.Name
				if prior, exists := seen[key]; exists {
					if declarationTokens(prior.Declaration) != declarationTokens(symbol.Declaration) {
						return Package{}, false, fmt.Errorf("platform-specific API mismatch for %s.%s: %q != %q", importPath, symbol.Name, prior.Declaration, symbol.Declaration)
					}
					continue
				}
				seen[key] = symbol
			}
		}
	}
	for _, symbol := range seen {
		pkg.Symbols = append(pkg.Symbols, symbol)
	}
	sort.Slice(pkg.Symbols, func(i, j int) bool {
		if pkg.Symbols[i].Name != pkg.Symbols[j].Name {
			return pkg.Symbols[i].Name < pkg.Symbols[j].Name
		}
		return pkg.Symbols[i].Kind < pkg.Symbols[j].Kind
	})
	return pkg, true, nil
}

func declarationTokens(declaration string) string {
	var source scanner.Scanner
	set := token.NewFileSet()
	file := set.AddFile("api.go", -1, len(declaration))
	source.Init(file, []byte(declaration), nil, scanner.ScanComments)
	var tokens strings.Builder
	for {
		_, item, literal := source.Scan()
		if item == token.EOF {
			return tokens.String()
		}
		tokens.WriteString(item.String())
		tokens.WriteByte(0)
		tokens.WriteString(literal)
		tokens.WriteByte(0)
	}
}

func exportedSymbols(fset *token.FileSet, declaration ast.Decl) []Symbol {
	switch typed := declaration.(type) {
	case *ast.FuncDecl:
		if !ast.IsExported(typed.Name.Name) {
			return nil
		}
		clone := *typed
		clone.Doc = nil
		clone.Body = nil
		kind := "function"
		name := typed.Name.Name
		if typed.Recv != nil && len(typed.Recv.List) != 0 {
			receiver := receiverName(typed.Recv.List[0].Type)
			if receiver == "" || !ast.IsExported(receiver) {
				return nil
			}
			kind = "method"
			name = receiver + "." + name
		}
		return []Symbol{{Kind: kind, Name: name, Declaration: formatNode(fset, &clone)}}
	case *ast.GenDecl:
		return exportedGeneral(fset, typed)
	default:
		return nil
	}
}

func exportedGeneral(fset *token.FileSet, declaration *ast.GenDecl) []Symbol {
	var symbols []Symbol
	for _, specification := range declaration.Specs {
		switch spec := specification.(type) {
		case *ast.TypeSpec:
			if !ast.IsExported(spec.Name.Name) {
				continue
			}
			clone := *spec
			clone.Doc, clone.Comment = nil, nil
			clone.Type = exportedTypeShape(spec.Type)
			general := &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&clone}}
			symbols = append(symbols, Symbol{Kind: "type", Name: spec.Name.Name, Declaration: formatNode(fset, general)})
		case *ast.ValueSpec:
			for index, name := range spec.Names {
				if !ast.IsExported(name.Name) {
					continue
				}
				clone := *spec
				clone.Doc, clone.Comment = nil, nil
				clone.Names = []*ast.Ident{{Name: name.Name}}
				switch {
				case len(spec.Values) == len(spec.Names):
					clone.Values = []ast.Expr{spec.Values[index]}
				case len(spec.Names) == 1:
					clone.Values = append([]ast.Expr(nil), spec.Values...)
				default:
					clone.Values = nil
				}
				kind := strings.ToLower(declaration.Tok.String())
				general := &ast.GenDecl{Tok: declaration.Tok, Specs: []ast.Spec{&clone}}
				symbols = append(symbols, Symbol{Kind: kind, Name: name.Name, Declaration: formatNode(fset, general)})
			}
		}
	}
	return symbols
}

func exportedTypeShape(expression ast.Expr) ast.Expr {
	switch typed := expression.(type) {
	case *ast.StructType:
		clone := *typed
		clone.Fields = exportedFieldList(typed.Fields)
		return &clone
	case *ast.InterfaceType:
		clone := *typed
		clone.Methods = exportedFieldList(typed.Methods)
		return &clone
	default:
		return expression
	}
}

func exportedFieldList(list *ast.FieldList) *ast.FieldList {
	if list == nil {
		return nil
	}
	clone := *list
	clone.List = nil
	for _, field := range list.List {
		fieldClone := *field
		fieldClone.Doc, fieldClone.Comment = nil, nil
		if len(field.Names) == 0 {
			if exportedEmbedded(field.Type) {
				clone.List = append(clone.List, &fieldClone)
			}
			continue
		}
		fieldClone.Names = nil
		for _, name := range field.Names {
			if ast.IsExported(name.Name) {
				fieldClone.Names = append(fieldClone.Names, &ast.Ident{Name: name.Name})
			}
		}
		if len(fieldClone.Names) != 0 {
			clone.List = append(clone.List, &fieldClone)
		}
	}
	return &clone
}

func exportedEmbedded(expression ast.Expr) bool {
	switch typed := expression.(type) {
	case *ast.Ident:
		return ast.IsExported(typed.Name)
	case *ast.SelectorExpr:
		return ast.IsExported(typed.Sel.Name)
	case *ast.StarExpr:
		return exportedEmbedded(typed.X)
	case *ast.IndexExpr:
		return exportedEmbedded(typed.X)
	case *ast.IndexListExpr:
		return exportedEmbedded(typed.X)
	case *ast.ParenExpr:
		return exportedEmbedded(typed.X)
	default:
		return false
	}
}

func receiverName(expression ast.Expr) string {
	switch typed := expression.(type) {
	case *ast.Ident:
		return typed.Name
	case *ast.StarExpr:
		return receiverName(typed.X)
	case *ast.IndexExpr:
		return receiverName(typed.X)
	case *ast.IndexListExpr:
		return receiverName(typed.X)
	case *ast.ParenExpr:
		return receiverName(typed.X)
	default:
		return ""
	}
}

func formatNode(fset *token.FileSet, node any) string {
	var buffer bytes.Buffer
	if err := format.Node(&buffer, fset, node); err != nil {
		panic(err)
	}
	return strings.TrimSpace(buffer.String())
}
