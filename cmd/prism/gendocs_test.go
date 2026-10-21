package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stellar-optics/stellar-prism/internal/cli"
)

func runWithDocs(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	var out, errBuf bytes.Buffer
	err = cli.Execute(args, &out, &errBuf, newGendocsCmd(&out, &errBuf))
	return out.String(), errBuf.String(), err
}

func TestGendocsMarkdown(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	stdout, _, err := runWithDocs(t, "gendocs", "markdown", "--dir", dir)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stdout, "markdown documentation") {
		t.Errorf("status does not name the format\n%s", stdout)
	}

	for _, name := range []string{"prism.md", "prism_events.md", "prism_abi.md", "prism_gendocs.md"} {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		if !strings.Contains(string(data), "prism") {
			t.Errorf("%s does not look like prism documentation", name)
		}
	}
}

func TestGendocsMan(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	_, _, err := runWithDocs(t, "gendocs", "man", "--dir", dir)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	for _, name := range []string{"prism.1", "prism-events.1", "prism-abi.1"} {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}
		if !strings.Contains(string(data), ".TH") {
			t.Errorf("%s is not a roff man page", name)
		}
	}
}

func TestGendocsRejectsBadArguments(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
	}{
		{name: "no format", args: []string{"gendocs"}},
		{name: "unknown format", args: []string{"gendocs", "html"}},
		{name: "too many formats", args: []string{"gendocs", "man", "markdown"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := runWithDocs(t, tc.args...); err == nil {
				t.Fatal("Execute() error = nil, want a validation failure")
			}
		})
	}
}

func TestGendocsRejectsEmptyAndUnusableDir(t *testing.T) {
	t.Parallel()

	if _, _, err := runWithDocs(t, "gendocs", "markdown", "--dir", ""); err == nil {
		t.Fatal("Execute() error = nil, want an empty --dir failure")
	}

	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("creating file: %v", err)
	}
	if _, _, err := runWithDocs(t, "gendocs", "markdown", "--dir", file); err == nil {
		t.Fatal("Execute() error = nil, want a directory creation failure")
	}
}

func TestGendocsAppearsInHelp(t *testing.T) {
	t.Parallel()
	stdout, _, err := runWithDocs(t, "--help")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stdout, "gendocs") {
		t.Errorf("root help does not mention gendocs\n%s", stdout)
	}
}
