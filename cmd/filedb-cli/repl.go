package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/peterh/liner"
	"github.com/spf13/cobra"
)

func replCmd(flags *cliFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "repl",
		Short: "Start interactive REPL",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runREPL(flags)
		},
	}
}

func runREPL(flags *cliFlags) error {
	line := liner.NewLiner()
	defer func() { _ = line.Close() }()
	line.SetCtrlCAborts(true)

	// Load history.
	histPath := filepath.Join(os.TempDir(), ".filedb_history")
	if f, err := os.Open(histPath); err == nil {
		_, _ = line.ReadHistory(f)
		_ = f.Close()
	}
	defer func() {
		if f, err := os.Create(histPath); err == nil {
			_, _ = line.WriteHistory(f)
			_ = f.Close()
		}
	}()

	collection := ""
	prompt := func() string {
		if collection == "" {
			return "filedb> "
		}
		return fmt.Sprintf("filedb:%s> ", collection)
	}

	fmt.Println("FileDB CLI — type 'help' for commands, 'exit' to quit")

	for {
		input, err := line.Prompt(prompt())
		if errors.Is(err, liner.ErrPromptAborted) || errors.Is(err, io.EOF) {
			fmt.Println()
			return nil
		}
		if err != nil {
			return err
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}
		line.AppendHistory(input)

		if err := handleREPLLine(input, &collection, flags); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
	}
}

// handleREPLLine parses and executes one REPL command line.
func handleREPLLine(input string, collection *string, flags *cliFlags) error {
	parts := tokenize(input)
	if len(parts) == 0 {
		return nil
	}
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "exit", "quit":
		fmt.Println("bye")
		os.Exit(0)

	case "help":
		printHelp()
		return nil

	case "use":
		if len(parts) < 2 {
			return fmt.Errorf("usage: use <collection>")
		}
		*collection = parts[1]
		fmt.Printf("switched to collection %q\n", *collection)
		return nil

	case "collections":
		return runCLICommand([]string{"collections"}, flags)

	case "create-collection", "create":
		if len(parts) < 2 {
			return fmt.Errorf("usage: create-collection <name>")
		}
		return runCLICommand([]string{"create-collection", parts[1]}, flags)

	case "drop-collection", "drop":
		if len(parts) < 2 {
			return fmt.Errorf("usage: drop-collection <name>")
		}
		return runCLICommand([]string{"drop-collection", parts[1]}, flags)

	case "insert":
		col, args, err := resolveCollection(parts[1:], *collection)
		if err != nil {
			return err
		}
		if len(args) < 1 {
			return fmt.Errorf("usage: insert [collection] <json>")
		}
		return runCLICommand([]string{"insert", col, args[0]}, flags)

	case "find":
		col, args, err := resolveCollection(parts[1:], *collection)
		if err != nil {
			return err
		}
		cmdArgs := []string{"find", col}
		if len(args) > 0 {
			cmdArgs = append(cmdArgs, args[0])
		}
		return runCLICommand(cmdArgs, flags)

	case "get":
		col, args, err := resolveCollection(parts[1:], *collection)
		if err != nil {
			return err
		}
		if len(args) < 1 {
			return fmt.Errorf("usage: get [collection] <id>")
		}
		return runCLICommand([]string{"get", col, args[0]}, flags)

	case "update":
		col, args, err := resolveCollection(parts[1:], *collection)
		if err != nil {
			return err
		}
		if len(args) < 2 {
			return fmt.Errorf("usage: update [collection] <id> <json>")
		}
		return runCLICommand([]string{"update", col, args[0], args[1]}, flags)

	case "delete":
		col, args, err := resolveCollection(parts[1:], *collection)
		if err != nil {
			return err
		}
		if len(args) < 1 {
			return fmt.Errorf("usage: delete [collection] <id>")
		}
		return runCLICommand([]string{"delete", col, args[0]}, flags)

	case "stats":
		col, _, err := resolveCollection(parts[1:], *collection)
		if err != nil {
			return err
		}
		return runCLICommand([]string{"stats", col}, flags)

	case "compact":
		col, _, err := resolveCollection(parts[1:], *collection)
		if err != nil {
			return err
		}
		return runCLICommand([]string{"compact", col}, flags)

	default:
		return fmt.Errorf("unknown command %q — type 'help' for a list", cmd)
	}
	return nil
}

// runCLICommand reuses the cobra command tree to execute a command.
func runCLICommand(args []string, flags *cliFlags) error {
	cmd := rootCmd()
	cmd.SetArgs(append([]string{
		"--host", flags.host,
		"--socket", flags.socket,
		"--api-key", flags.apiKey,
	}, args...))
	return cmd.Execute()
}

// resolveCollection extracts the collection name either from args or from the
// currently active REPL collection.
func resolveCollection(args []string, current string) (string, []string, error) {
	if current != "" {
		return current, args, nil
	}
	if len(args) == 0 {
		return "", nil, fmt.Errorf("no collection selected — use 'use <collection>' first")
	}
	return args[0], args[1:], nil
}

// tokenize splits a command line, respecting quoted strings.
func tokenize(s string) []string {
	var tokens []string
	var current strings.Builder
	inQuote := false

	for _, r := range s {
		switch {
		case r == '"' || r == '\'':
			inQuote = !inQuote
			current.WriteRune(r)
		case r == ' ' && !inQuote:
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

func printHelp() {
	fmt.Print(`
Commands:
  use <collection>                    Switch active collection
  collections                         List all collections
  create-collection <name>            Create a collection
  drop-collection <name>              Drop a collection
  insert [collection] <json>          Insert a record
  find [collection] [filter-json]     Find records
  get [collection] <id>               Get record by id
  update [collection] <id> <json>     Update a record
  delete [collection] <id>            Delete a record
  stats [collection]                  Show collection stats
  compact [collection]                Force a compaction pass
  help                                Show this help
  exit / quit                         Exit

Filter JSON examples:
  {"field":"name","op":"eq","value":"alice"}
  {"and":[{"field":"age","op":"gte","value":"18"},{"field":"city","op":"eq","value":"NYC"}]}
  {"field":"email","op":"contains","value":"@gmail"}
`)
}
