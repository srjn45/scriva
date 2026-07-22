package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func runCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "run [file]",
		Short: "Execute a .fql batch script (or read from stdin if no file given)",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var r io.Reader
			if len(args) == 1 {
				f, err := os.Open(args[0])
				if err != nil {
					return fmt.Errorf("open script: %w", err)
				}
				defer func() { _ = f.Close() }()
				r = f
			} else {
				// Check if stdin has data (piped input).
				stat, _ := os.Stdin.Stat()
				if (stat.Mode() & os.ModeCharDevice) != 0 {
					return fmt.Errorf("run: no file given and stdin is not piped")
				}
				r = os.Stdin
			}
			return runBatch(r, flags, cmd.OutOrStdout())
		},
	}
}

// runBatch executes each non-blank, non-comment line from r as a REPL command.
func runBatch(r io.Reader, flags *cliFlags, out io.Writer) error {
	collection := ""
	scanner := bufio.NewScanner(r)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip blank lines and comments.
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		_, _ = fmt.Fprintf(out, ">>> %s\n", line)
		if err := handleREPLLine(line, &collection, flags); err != nil {
			_, _ = fmt.Fprintf(out, "error on line %d: %v\n", lineNum, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("batch read: %w", err)
	}
	return nil
}
