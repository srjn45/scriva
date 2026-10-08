package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/srjn45/scriva/engine"
)

// Exit codes for the offline integrity commands.
const (
	exitClean      = 0 // nothing above informational severity
	exitRepairable = 1 // only derived structures disagree; `scriva repair` fixes it
	exitCorrupt    = 2 // damaged segment bytes or conflicting record history
	exitUsage      = 3 // bad flags, unreadable directory, or the directory is locked
)

// exitError carries a process exit code out of a cobra RunE.
type exitError struct {
	code int
	msg  string
}

func (e *exitError) Error() string { return e.msg }

func usageErr(format string, args ...any) error {
	return &exitError{code: exitUsage, msg: fmt.Sprintf(format, args...)}
}

func severityExit(s engine.Severity) int {
	switch s {
	case engine.SeverityRepairableIndex:
		return exitRepairable
	case engine.SeverityDataCorruption, engine.SeverityConflict:
		return exitCorrupt
	}
	return exitClean
}

func integrityCtx() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func collections(name string) []string {
	if name == "" {
		return nil
	}
	return []string{name}
}

// quietUsage keeps cobra from dumping usage for failures that are not flag
// mistakes, and maps flag mistakes to the usage exit code.
func quietUsage(cmd *cobra.Command) {
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
		_, _ = fmt.Fprintf(c.ErrOrStderr(), "error: %v\n", err)
		return usageErr("%v", err)
	})
}

func verifyCmd() *cobra.Command {
	var (
		dataDir, collection, mode string
		asJSON                    bool
	)
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Check a data directory for corruption (offline, read-only)",
		Long: `Verify checks the segments and persisted index files of a data directory
without modifying anything. Run it against a stopped server (or a copy); a
directory held open by a live process is reported and findings may reflect
in-flight writes.

Exit codes: 0 clean, 1 repairable (run "scriva repair"), 2 data corruption or
conflicting history, 3 usage error / directory unreadable.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if dataDir == "" {
				return usageErr("--data is required")
			}
			vm := engine.VerifyMode(mode)
			if vm != engine.VerifyQuick && vm != engine.VerifyFull {
				return usageErr("invalid --mode %q (want quick or full)", mode)
			}
			ctx, stop := integrityCtx()
			defer stop()
			rep, err := engine.VerifyDir(ctx, dataDir, engine.VerifyOptions{Mode: vm, Collections: collections(collection)})
			if err != nil {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "error: %v\n", err)
				return usageErr("%v", err)
			}
			code := severityExit(rep.MaxSeverity())
			if asJSON {
				if err := writeJSON(cmd.OutOrStdout(), struct {
					*engine.IntegrityReport
					Status   string `json:"status"`
					ExitCode int    `json:"exit_code"`
				}{rep, statusName(code), code}); err != nil {
					return err
				}
			} else {
				printVerify(cmd.OutOrStdout(), dataDir, rep, code)
			}
			if code != exitClean {
				return &exitError{code: code, msg: "verify: " + statusName(code)}
			}
			return nil
		},
	}
	quietUsage(cmd)
	f := cmd.Flags()
	f.StringVar(&dataDir, "data", "", "Data directory to verify (required)")
	f.StringVar(&collection, "collection", "", "Verify only this collection (default: all)")
	f.StringVar(&mode, "mode", "full", "Verification depth: quick or full")
	f.BoolVar(&asJSON, "json", false, "Emit the report as JSON")
	return cmd
}

func statusName(code int) string {
	switch code {
	case exitClean:
		return "clean"
	case exitRepairable:
		return "repairable"
	case exitCorrupt:
		return "corrupt"
	}
	return "error"
}

func printFinding(w io.Writer, f engine.Finding) {
	loc := ""
	if f.Location.Segment != "" {
		loc = " " + f.Location.Segment
		if f.Location.Offset != 0 {
			loc += fmt.Sprintf("@%d", f.Location.Offset)
		}
	}
	_, _ = fmt.Fprintf(w, "    [%s] %s%s: %s\n", f.Severity, f.Code, loc, f.Message)
}

func printCollectionReport(w io.Writer, cr engine.CollectionReport) {
	_, _ = fmt.Fprintf(w, "  collection %s: %d segment(s), %d live record(s), %d finding(s)\n",
		cr.Name, cr.Stats.Segments, cr.Stats.LiveRecords, len(cr.Findings))
	for _, f := range cr.Findings {
		printFinding(w, f)
	}
	for code, n := range cr.Truncated {
		_, _ = fmt.Fprintf(w, "    ... %d more %s finding(s) not shown\n", n, code)
	}
}

func printVerify(w io.Writer, dir string, rep *engine.IntegrityReport, code int) {
	_, _ = fmt.Fprintf(w, "verify %s (mode %s)\n", dir, rep.Mode)
	for _, f := range rep.Findings {
		printFinding(w, f)
	}
	for _, cr := range rep.Collections {
		printCollectionReport(w, cr)
	}
	_, _ = fmt.Fprintf(w, "result: %s (exit %d)\n", statusName(code), code)
	switch code {
	case exitRepairable:
		_, _ = fmt.Fprintf(w, "next: scriva repair --data %s --dry-run   (then without --dry-run)\n", dir)
	case exitCorrupt:
		_, _ = fmt.Fprintf(w, "next: scriva repair --data %s --dry-run   (damaged segments need --salvage; conflicts are never auto-resolved)\n", dir)
	}
}

func repairCmd() *cobra.Command {
	var (
		dataDir, collection, onConflict string
		salvage, dryRun, asJSON         bool
		backupDir                       string
	)
	cmd := &cobra.Command{
		Use:   "repair",
		Short: "Repair a data directory offline (verified backup first)",
		Long: `Repair rebuilds derived structures (indexes, id counter, interrupted
compaction swaps) from the segments, which are the source of truth. Before the
first change it takes a verified backup next to the data directory. Conflicting
history is never resolved, only reported (or, with --on-conflict abort, refused
before anything is touched). Damaged segments are only rewritten with --salvage.

The server must be stopped: repair refuses a directory that is open elsewhere.
--dry-run verifies and prints the plan without modifying anything.

Exit codes: 0 clean/repaired, 1 (dry-run) repairs would be applied, 2 corruption
or conflicts remain, 3 usage error / directory locked.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if dataDir == "" {
				return usageErr("--data is required")
			}
			policy := engine.ConflictPolicy(onConflict)
			if policy != engine.ConflictReport && policy != engine.ConflictAbort {
				return usageErr("invalid --on-conflict %q (want report or abort)", onConflict)
			}
			ctx, stop := integrityCtx()
			defer stop()
			if dryRun {
				return runRepairDryRun(ctx, cmd, dataDir, collection, salvage, policy, asJSON)
			}
			rep, err := engine.Repair(ctx, dataDir, engine.RepairOptions{
				Collections: collections(collection), BackupDir: backupDir, OnConflict: policy, Salvage: salvage,
			})
			code := repairExit(rep, err)
			if rep == nil {
				_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "error: %v\n", err)
				if errors.Is(err, engine.ErrDatabaseLocked) {
					_, _ = fmt.Fprintln(cmd.ErrOrStderr(), "the data directory is open in another process; stop the server and retry")
				}
				return &exitError{code: code, msg: err.Error()}
			}
			if asJSON {
				if jerr := writeJSON(cmd.OutOrStdout(), struct {
					*engine.RepairReport
					Status   string `json:"status"`
					Error    string `json:"error,omitempty"`
					ExitCode int    `json:"exit_code"`
				}{rep, statusName(code), errString(err), code}); jerr != nil {
					return jerr
				}
			} else {
				printRepair(cmd.OutOrStdout(), rep, err, code)
			}
			if code != exitClean {
				return &exitError{code: code, msg: "repair: " + statusName(code)}
			}
			return nil
		},
	}
	quietUsage(cmd)
	f := cmd.Flags()
	f.StringVar(&dataDir, "data", "", "Data directory to repair (required)")
	f.StringVar(&collection, "collection", "", "Repair only this collection (default: all)")
	f.BoolVar(&salvage, "salvage", false, "Move valid records out of damaged segments into a new segment (originals stay in the backup)")
	f.StringVar(&onConflict, "on-conflict", "report", "Conflicting history policy: report or abort")
	f.BoolVar(&dryRun, "dry-run", false, "Print the plan without modifying anything")
	f.BoolVar(&asJSON, "json", false, "Emit the report as JSON")
	f.StringVar(&backupDir, "backup-dir", "", "Parent directory for the backup (default: parent of --data; must be outside it)")
	return cmd
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// repairExit classifies a Repair outcome. A nil report means nothing ran:
// lock and bad-directory failures are usage-class, anything else a plain failure.
func repairExit(rep *engine.RepairReport, err error) int {
	if rep == nil {
		if errors.Is(err, engine.ErrDatabaseLocked) || strings.Contains(err.Error(), "not a directory") ||
			strings.Contains(err.Error(), "backup dir") || strings.Contains(err.Error(), "read ") {
			return exitUsage
		}
		return 1
	}
	if errors.Is(err, engine.ErrRepairConflict) || errors.Is(err, engine.ErrRepairIncomplete) {
		return exitCorrupt
	}
	if err != nil {
		return 1
	}
	for _, c := range rep.Collections {
		if len(c.Conflicts) > 0 {
			return exitCorrupt
		}
		if c.After != nil && !isClean(c.After) {
			return severityExit(maxSeverity(c.After.Findings))
		}
	}
	return exitClean
}

func isClean(cr *engine.CollectionReport) bool {
	return severityExit(maxSeverity(cr.Findings)) == exitClean
}

func maxSeverity(fs []engine.Finding) engine.Severity {
	r := engine.IntegrityReport{Findings: fs}
	return r.MaxSeverity()
}

func printRepair(w io.Writer, rep *engine.RepairReport, err error, code int) {
	_, _ = fmt.Fprintf(w, "repair %s\n", rep.Dir)
	if rep.Resumed {
		_, _ = fmt.Fprintln(w, "  resumed an interrupted repair")
	}
	for _, c := range rep.Collections {
		_, _ = fmt.Fprintf(w, "  collection %s: %s\n", c.Name, c.Status)
		if c.Reason != "" {
			_, _ = fmt.Fprintf(w, "    reason: %s\n", c.Reason)
		}
		for _, a := range c.Actions {
			_, _ = fmt.Fprintf(w, "    action: %s %s %s\n", a.Kind, a.Target, a.Detail)
		}
		if s := c.Salvage; s != nil {
			_, _ = fmt.Fprintf(w, "    salvage: %d entries into %s, %d bad region(s) lost, quarantined %v\n",
				s.Entries, s.NewSegment, s.BadRegions, s.Quarantined)
		}
		for _, f := range c.Conflicts {
			printFinding(w, f)
		}
	}
	if rep.BackupDir != "" {
		_, _ = fmt.Fprintf(w, "backup: %s\n", rep.BackupDir)
	}
	if err != nil {
		_, _ = fmt.Fprintf(w, "note: %v\n", err)
	}
	_, _ = fmt.Fprintf(w, "result: %s (exit %d)\n", statusName(code), code)
	if rep.BackupDir != "" {
		_, _ = fmt.Fprintf(w, "next: scriva verify --data %s ; keep %s until you are satisfied\n", rep.Dir, rep.BackupDir)
	}
	if code == exitCorrupt {
		_, _ = fmt.Fprintln(w, "next: remaining corruption/conflicts need manual review (damaged segments: rerun with --salvage)")
	}
}

// runRepairDryRun verifies (read-only) and describes what Repair would do,
// using the same decision rules Repair applies per severity.
func runRepairDryRun(ctx context.Context, cmd *cobra.Command, dir, collection string, salvage bool, policy engine.ConflictPolicy, asJSON bool) error {
	rep, err := engine.VerifyDir(ctx, dir, engine.VerifyOptions{Mode: engine.VerifyFull, Collections: collections(collection)})
	if err != nil {
		_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "error: %v\n", err)
		return usageErr("%v", err)
	}
	type planItem struct {
		Collection string   `json:"collection"`
		Plan       string   `json:"plan"`
		Details    []string `json:"details,omitempty"`
	}
	var plan []planItem
	code := exitClean
	conflicts := false
	for _, c := range rep.Collections {
		if severityExit(maxSeverity(c.Findings)) == exitCorrupt && maxSeverity(c.Findings) == engine.SeverityConflict {
			conflicts = true
		}
	}
	for _, c := range rep.Collections {
		sev := maxSeverity(c.Findings)
		item := planItem{Collection: c.Name}
		switch {
		case sev == engine.SeverityConflict:
			if policy == engine.ConflictAbort {
				item.Plan = "abort: conflicting history (--on-conflict abort); nothing is modified"
			} else {
				item.Plan = "blocked: conflicting history is never resolved automatically"
			}
			code = exitCorrupt
		case conflicts && policy == engine.ConflictAbort:
			item.Plan = "abort: another collection has conflicting history; nothing is modified"
			code = exitCorrupt
		case sev == engine.SeverityDataCorruption:
			if salvage {
				item.Plan = "backup, salvage valid records into a new segment, rebuild indexes"
				if code == exitClean {
					code = exitRepairable
				}
			} else {
				item.Plan = "blocked: damaged segment bytes (rerun with --salvage)"
				code = exitCorrupt
			}
		case sev == engine.SeverityRepairableIndex:
			item.Plan = "backup, rebuild derived structures from segments"
			if code == exitClean {
				code = exitRepairable
			}
		default:
			item.Plan = "no change"
		}
		for _, f := range c.Findings {
			if f.Severity != engine.SeverityInfo {
				item.Details = append(item.Details, fmt.Sprintf("[%s] %s: %s", f.Severity, f.Code, f.Message))
			}
		}
		plan = append(plan, item)
	}
	if asJSON {
		if err := writeJSON(cmd.OutOrStdout(), struct {
			Dir      string     `json:"dir"`
			DryRun   bool       `json:"dry_run"`
			Plan     []planItem `json:"plan"`
			Status   string     `json:"status"`
			ExitCode int        `json:"exit_code"`
		}{dir, true, plan, statusName(code), code}); err != nil {
			return err
		}
	} else {
		w := cmd.OutOrStdout()
		_, _ = fmt.Fprintf(w, "repair --dry-run %s (nothing will be modified)\n", dir)
		for _, p := range plan {
			_, _ = fmt.Fprintf(w, "  collection %s: %s\n", p.Collection, p.Plan)
			for _, d := range p.Details {
				_, _ = fmt.Fprintf(w, "    %s\n", d)
			}
		}
		_, _ = fmt.Fprintf(w, "result: %s (exit %d)\n", statusName(code), code)
		if code == exitRepairable {
			_, _ = fmt.Fprintf(w, "next: scriva repair --data %s   (a verified backup is taken first)\n", dir)
		}
	}
	if code != exitClean {
		return &exitError{code: code, msg: "repair dry-run: " + statusName(code)}
	}
	return nil
}
