package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"

	"gmountie/test/e2e/perf/bmf"
)

// plots implements `perfbmf plots sync`: reconcile the live Bencher dashboard
// to match a declarative plots.yaml. The diff/plan is bmf.Plan (pure, tested);
// everything here is the I/O shell that fetches live state and applies actions
// by shelling out to the `bencher` CLI (which handles auth via $BENCHER_API_TOKEN).
func plots(args []string) {
	if len(args) < 1 || args[0] != "sync" {
		fail("usage: perfbmf plots sync [--spec f] [--project p] [--dry-run] [--prune]")
	}
	fs := flag.NewFlagSet("plots sync", flag.ExitOnError)
	specPath := fs.String("spec", "scripts/perf/plots.yaml", "plots spec YAML")
	project := fs.String("project", os.Getenv("BENCHER_PROJECT"), "Bencher project slug (default $BENCHER_PROJECT)")
	dryRun := fs.Bool("dry-run", false, "print the plan without applying it")
	prune := fs.Bool("prune", false, "delete live plots whose title is not in the spec")
	_ = fs.Parse(args[1:])

	if *project == "" {
		fail("--project (or $BENCHER_PROJECT) is required")
	}

	f := mustOpen(*specPath)
	sp, err := bmf.LoadSpec(f)
	_ = f.Close()
	if err != nil {
		fail("%v", err)
	}
	norm, err := sp.Normalize()
	if err != nil {
		fail("%v", err)
	}

	nm := bmf.NameMaps{
		Benchmarks: listNameMap(*project, "benchmark"),
		Measures:   listNameMap(*project, "measure"),
		Branches:   listNameMap(*project, "branch"),
		Testbeds:   listNameMap(*project, "testbed"),
	}
	actual := listPlots(*project)

	desired, err := bmf.Resolve(norm, nm)
	if err != nil {
		fail("%v", err)
	}

	actions := bmf.Plan(desired, actual, *prune)
	printPlan(actions)

	if *dryRun {
		fmt.Println("\n(dry-run; no changes applied)")
		return
	}
	applyPlan(*project, actions)
}

func printPlan(actions []bmf.Action) {
	fmt.Println("Plan:")
	changes := 0
	for _, a := range actions {
		if a.Kind != bmf.Noop {
			changes++
		}
		n := 0
		if a.Desired != nil {
			n = len(a.Desired.Benchmarks)
		}
		if a.Kind == bmf.Delete {
			fmt.Printf("  %-8s %q  (%s)\n", a.Kind, a.Title, a.Reason)
		} else {
			fmt.Printf("  %-8s %q  index=%d benches=%d  (%s)\n", a.Kind, a.Title, a.Index, n, a.Reason)
		}
	}
	fmt.Printf("\n%d action(s), %d change(s)\n", len(actions), changes)
}

func applyPlan(project string, actions []bmf.Action) {
	for _, a := range actions {
		switch a.Kind {
		case bmf.Noop:
			// Content matches, but the GET API never returns the index — so the
			// dashboard order is enforced unconditionally from spec order here.
			bencherOut("plot", "update", project, a.OldUUID, "--index", strconv.Itoa(a.Index))
		case bmf.UpdateMeta:
			bencherOut("plot", "update", project, a.OldUUID,
				"--index", strconv.Itoa(a.Index),
				"--window", strconv.FormatInt(a.Desired.Window, 10))
		case bmf.Create:
			createPlot(project, a)
		case bmf.Recreate:
			// Create the replacement first; only delete the old one once the new
			// one exists, so a failed create never leaves the plot missing.
			createPlot(project, a)
			bencherOut("plot", "delete", project, a.OldUUID)
		case bmf.Delete:
			bencherOut("plot", "delete", project, a.OldUUID)
		}
		fmt.Printf("  applied %-8s %q\n", a.Kind, a.Title)
	}
}

func createPlot(project string, a bmf.Action) {
	d := a.Desired
	args := []string{"plot", "create", project,
		"--title", d.Title,
		"--index", strconv.Itoa(a.Index),
		"--window", strconv.FormatInt(d.Window, 10),
		"--x-axis", d.XAxis,
		"--branches", d.Branch,
		"--testbeds", d.Testbed,
		"--measures", d.Measure,
	}
	if d.LowerValue {
		args = append(args, "--lower-value")
	}
	if d.UpperValue {
		args = append(args, "--upper-value")
	}
	if d.LowerBoundary {
		args = append(args, "--lower-boundary")
	}
	if d.UpperBoundary {
		args = append(args, "--upper-boundary")
	}
	for _, u := range d.Benchmarks {
		args = append(args, "--benchmarks", u)
	}
	bencherOut(args...)
}

// bencherOut runs `bencher <args>` and returns stdout, failing the process on a
// non-zero exit (stderr is surfaced in the error).
func bencherOut(args ...string) []byte {
	cmd := exec.CommandContext(context.Background(), "bencher", args...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		fail("bencher %v: %v\n%s", args, err, errb.String())
	}
	return out.Bytes()
}

// listNameMap builds {name,slug}->uuid from `bencher <kind> list`. Both keys are
// registered because Bencher titles built-in measures ("Throughput", "Latency")
// while custom ones keep their BMF slug ("throughput_pct_of_raw"); mapping both
// lets the spec use the natural lowercase slug uniformly. Benchmark globs match
// against names (slugs have no "/"), so the extra slug keys are harmless there.
func listNameMap(project, kind string) map[string]string {
	var items []struct {
		Name string `json:"name"`
		Slug string `json:"slug"`
		UUID string `json:"uuid"`
	}
	if err := json.Unmarshal(bencherOut(kind, "list", project, "--per-page", "255"), &items); err != nil {
		fail("decode %s list: %v", kind, err)
	}
	m := make(map[string]string, 2*len(items))
	for _, it := range items {
		if it.Name != "" {
			m[it.Name] = it.UUID
		}
		if it.Slug != "" {
			m[it.Slug] = it.UUID
		}
	}
	return m
}

func listPlots(project string) []bmf.Plot {
	var ps []bmf.Plot
	if err := json.Unmarshal(bencherOut("plot", "list", project, "--per-page", "255"), &ps); err != nil {
		fail("decode plot list: %v", err)
	}
	return ps
}
