package cli

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

// completionShells lists the shells prism can generate completions for.
var completionShells = []string{"bash", "zsh", "fish", "powershell"}

// newCompletionCmd builds `prism completion <shell>`.
//
// Generation is pure: it only walks the command tree, so it never contacts the
// RPC endpoint and works with no network. Flag value completions are
// registered by the commands themselves (see registerFlagCompletion).
func newCompletionCmd(stdout io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "completion [bash|zsh|fish|powershell]",
		Short: "Generate a shell autocompletion script",
		Long: `Generate an autocompletion script for prism.

The script includes static command and flag completion, plus suggestions for
enumerated flag values such as --color, --type and --gap-strategy. Source the
output in your shell's startup file, for example:

  prism completion bash > /etc/bash_completion.d/prism
  prism completion zsh  > "${fpath[1]}/_prism"
  prism completion fish > ~/.config/fish/completions/prism.fish`,
		Example: `  # Bash, loaded for the current session
  source <(prism completion bash)

  # Zsh
  prism completion zsh > "${fpath[1]}/_prism"

  # Fish
  prism completion fish > ~/.config/fish/completions/prism.fish

  # PowerShell
  prism completion powershell | Out-String | Invoke-Expression`,
		Args:                  cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		ValidArgs:             completionShells,
		ValidArgsFunction:     fixedCompletions(completionShells...),
		DisableFlagsInUseLine: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			root := cmd.Root()
			switch args[0] {
			case "bash":
				return root.GenBashCompletionV2(stdout, true)
			case "zsh":
				return root.GenZshCompletion(stdout)
			case "fish":
				return root.GenFishCompletion(stdout, true)
			case "powershell":
				return root.GenPowerShellCompletionWithDesc(stdout)
			default:
				// Unreachable: OnlyValidArgs rejects anything else before RunE.
				return fmt.Errorf("unsupported shell %q: want one of %v", args[0], completionShells)
			}
		},
	}
	return cmd
}

// fixedCompletions returns the same suggestions for every invocation and
// tells the shell not to fall back to filenames.
func fixedCompletions(values ...string) cobra.CompletionFunc {
	choices := append([]cobra.Completion(nil), values...)
	return func(cmd *cobra.Command, args []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
		return choices, cobra.ShellCompDirectiveNoFileComp
	}
}

// registerFlagCompletion attaches dynamic value suggestions to a flag.
//
// Every call site registers a flag that was just defined on cmd, so a failure
// can only mean a programming error. Returning it would force every command
// constructor to grow an error path for something tests catch immediately.
func registerFlagCompletion(cmd *cobra.Command, flag string, values ...string) {
	if err := cmd.RegisterFlagCompletionFunc(flag, fixedCompletions(values...)); err != nil {
		panic(fmt.Sprintf("prism: registering completion for --%s: %v", flag, err))
	}
}
