package cli

import (
	"strings"
	"testing"
)

func TestCompletionGenerator(t *testing.T) {
	tests := []struct {
		shell string
		want  string
	}{
		{shell: "bash", want: "__start_prism"},
		{shell: "zsh", want: "#compdef prism"},
		{shell: "fish", want: "complete -c prism"},
		{shell: "powershell", want: "Register-ArgumentCompleter"},
	}

	for _, tc := range tests {
		t.Run(tc.shell, func(t *testing.T) {
			t.Parallel()
			stdout, _, err := run(t, "completion", tc.shell)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if !strings.Contains(stdout, tc.want) {
				t.Errorf("%s completion does not contain %q\n%s", tc.shell, tc.want, stdout)
			}
		})
	}
}

func TestCompletionRejectsUnknownShell(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
	}{
		{name: "no shell", args: []string{"completion"}},
		{name: "unknown shell", args: []string{"completion", "elvish"}},
		{name: "too many", args: []string{"completion", "bash", "zsh"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := run(t, tc.args...); err == nil {
				t.Fatal("Execute() error = nil, want a validation failure")
			}
		})
	}
}

func TestCompletionSuggestsEnumeratedFlagValues(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "event type",
			args: []string{"__complete", "events", "--type", ""},
			want: []string{"contract", "system", "diagnostic"},
		},
		{
			name: "gap strategy",
			args: []string{"__complete", "events", "--gap-strategy", ""},
			want: []string{"fail", "resume"},
		},
		{
			name: "colour",
			args: []string{"__complete", "--color", ""},
			want: []string{"auto", "always", "never"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stdout, _, err := run(t, tc.args...)
			if err != nil {
				// Cobra's __complete command returns nil errors even when it
				// has suggestions; a failure here means the wiring is broken.
				t.Fatalf("Execute() error = %v", err)
			}
			for _, want := range tc.want {
				if !strings.Contains(stdout, want+"\n") {
					t.Errorf("completion output does not suggest %q\n%s", want, stdout)
				}
			}
			if !strings.Contains(stdout, "ShellCompDirectiveNoFileComp") &&
				!strings.Contains(stdout, ":4") {
				t.Errorf("completion did not request no-file completion\n%s", stdout)
			}
		})
	}
}

func TestCompletionIsAdvertisedInHelp(t *testing.T) {
	stdout, _, err := run(t, "--help")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stdout, "completion") {
		t.Errorf("root help does not mention completion\n%s", stdout)
	}
}
