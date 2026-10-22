package main

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

// newGendocsCmd builds `prism gendocs <man|markdown>`.
//
// The command lives in package main because it is a build/release helper, not
// part of the streaming CLI surface. main attaches it to the root command so
// it appears in --help and in generated completion scripts, but internal/cli
// does not need to depend on cobra/doc.
func newGendocsCmd(stdout, stderr io.Writer) *cobra.Command {
	var dir string

	cmd := &cobra.Command{
		Use:   "gendocs [man|markdown]",
		Short: "Generate man pages or Markdown command reference",
		Long: `Generate reference documentation for every prism command.

"markdown" writes one .md file per command into --dir, suitable for a static
site. "man" writes one roff man page per command, suitable for installation
under /usr/share/man/man1.`,
		Example: `  prism gendocs markdown --dir docs/reference
  prism gendocs man --dir docs/man`,
		Args:      cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs),
		ValidArgs: []string{"man", "markdown"},
		ValidArgsFunction: func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return []string{"man", "markdown"}, cobra.ShellCompDirectiveNoFileComp
		},
		DisableFlagsInUseLine: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if dir == "" {
				return fmt.Errorf("--dir must not be empty")
			}
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return fmt.Errorf("creating output directory %s: %w", dir, err)
			}

			root := cmd.Root()
			// A generated timestamp makes every run dirty in version control.
			// Disabling the tag keeps the output reproducible.
			root.DisableAutoGenTag = true

			switch args[0] {
			case "markdown":
				if err := doc.GenMarkdownTree(root, dir); err != nil {
					return fmt.Errorf("generating Markdown docs: %w", err)
				}
			case "man":
				header := &doc.GenManHeader{
					Title:   "PRISM",
					Section: "1",
					Source:  "prism",
					Manual:  "prism manual",
				}
				if err := doc.GenManTree(root, header, dir); err != nil {
					return fmt.Errorf("generating man pages: %w", err)
				}
			default:
				// Unreachable: OnlyValidArgs rejects anything else before RunE.
				return fmt.Errorf("unsupported documentation format %q", args[0])
			}

			_, err := fmt.Fprintf(stdout, "prism: wrote %s documentation to %s\n", args[0], dir)
			if err != nil {
				return fmt.Errorf("writing status: %w", err)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&dir, "dir", "docs", "directory to write generated documentation into")
	cmd.SetOut(stdout)
	cmd.SetErr(stderr)
	return cmd
}
