package bmf

import (
	"fmt"
	"io"
	"path"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// This file owns the declarative side of the Bencher dashboard: a YAML spec of
// the plots we want, plus a pure planner that diffs that desired state against
// the live plots and emits the actions to converge. The executor (which shells
// out to the `bencher` CLI) lives in cmd/perfbmf; everything here is pure so the
// diff logic — where the bugs hide — is unit-testable.
//
// Why a planner at all: `bencher plot update` can only change title/index/
// window, never a plot's benchmark or measure set, and new benchmarks never
// auto-join an existing plot. So adding a benchmark means delete + recreate.
// Referencing benchmarks by name-glob (e.g. "Seq*MiB/lan") means a future
// SeqReadOpt128MiB is picked up on the next sync without editing UUIDs by hand.

// SpecDefaults are applied to any PlotSpec field left empty.
type SpecDefaults struct {
	Branch  string `yaml:"branch"`
	Testbed string `yaml:"testbed"`
	Window  int64  `yaml:"window"`
	XAxis   string `yaml:"x_axis"`
}

// PlotSpec is one plot's declarative definition. Benchmarks are name globs
// (path.Match syntax, "/"-aware) resolved against the live benchmark list.
type PlotSpec struct {
	Title         string   `yaml:"title"`
	Measure       string   `yaml:"measure"` // measure name (e.g. "throughput")
	Benchmarks    []string `yaml:"benchmarks"`
	Branch        string   `yaml:"branch"`  // optional; falls back to defaults
	Testbed       string   `yaml:"testbed"` // optional; falls back to defaults
	Window        int64    `yaml:"window"`  // optional; falls back to defaults
	XAxis         string   `yaml:"x_axis"`  // optional; falls back to defaults
	LowerValue    bool     `yaml:"lower_value"`
	UpperValue    bool     `yaml:"upper_value"`
	LowerBoundary bool     `yaml:"lower_boundary"`
	UpperBoundary bool     `yaml:"upper_boundary"`
}

// Spec is the whole plots.yaml document.
type Spec struct {
	Defaults SpecDefaults `yaml:"defaults"`
	Plots    []PlotSpec   `yaml:"plots"`
}

// LoadSpec decodes a plots.yaml document.
func LoadSpec(r io.Reader) (Spec, error) {
	var sp Spec
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true) // typo in a key is a hard error, not a silent default
	if err := dec.Decode(&sp); err != nil {
		return Spec{}, fmt.Errorf("decode spec: %w", err)
	}
	return sp, nil
}

// Normalize applies defaults to every plot and validates required fields. The
// returned slice preserves document order — that order is the dashboard index.
func (sp Spec) Normalize() ([]PlotSpec, error) {
	out := make([]PlotSpec, 0, len(sp.Plots))
	seen := map[string]bool{}
	for i, p := range sp.Plots {
		if p.Branch == "" {
			p.Branch = sp.Defaults.Branch
		}
		if p.Testbed == "" {
			p.Testbed = sp.Defaults.Testbed
		}
		if p.Window == 0 {
			p.Window = sp.Defaults.Window
		}
		if p.XAxis == "" {
			p.XAxis = sp.Defaults.XAxis
		}
		switch {
		case p.Title == "":
			return nil, fmt.Errorf("plot[%d]: title is required", i)
		case p.Measure == "":
			return nil, fmt.Errorf("plot %q: measure is required", p.Title)
		case len(p.Benchmarks) == 0:
			return nil, fmt.Errorf("plot %q: at least one benchmark glob is required", p.Title)
		case p.Branch == "":
			return nil, fmt.Errorf("plot %q: branch is required (set it or defaults.branch)", p.Title)
		case p.Testbed == "":
			return nil, fmt.Errorf("plot %q: testbed is required (set it or defaults.testbed)", p.Title)
		case seen[p.Title]:
			return nil, fmt.Errorf("plot %q: duplicate title (title is the match key, must be unique)", p.Title)
		}
		seen[p.Title] = true
		out = append(out, p)
	}
	return out, nil
}

// NameMaps translate human names to Bencher UUIDs, built from `bencher * list`.
type NameMaps struct {
	Benchmarks map[string]string // benchmark name -> uuid
	Measures   map[string]string
	Branches   map[string]string
	Testbeds   map[string]string
}

// ResolvedPlot is a desired plot in UUID space, ready to diff and create.
type ResolvedPlot struct {
	Title                                                string
	Measure                                              string   // uuid
	Benchmarks                                           []string // uuids, sorted+deduped
	Branch                                               string   // uuid
	Testbed                                              string   // uuid
	Window                                               int64
	XAxis                                                string
	LowerValue, UpperValue, LowerBoundary, UpperBoundary bool
}

// Resolve turns normalized PlotSpecs into UUID-space ResolvedPlots, expanding
// benchmark globs against nm.Benchmarks. A glob that matches zero benchmarks is
// a hard error — silently producing an empty chart hides the real problem.
func Resolve(plots []PlotSpec, nm NameMaps) ([]ResolvedPlot, error) {
	allNames := make([]string, 0, len(nm.Benchmarks))
	for n := range nm.Benchmarks {
		allNames = append(allNames, n)
	}
	sort.Strings(allNames)

	out := make([]ResolvedPlot, 0, len(plots))
	for _, p := range plots {
		measure, ok := nm.Measures[p.Measure]
		if !ok {
			return nil, fmt.Errorf("plot %q: unknown measure %q (known: %s)", p.Title, p.Measure, keys(nm.Measures))
		}
		branch, ok := nm.Branches[p.Branch]
		if !ok {
			return nil, fmt.Errorf("plot %q: unknown branch %q", p.Title, p.Branch)
		}
		testbed, ok := nm.Testbeds[p.Testbed]
		if !ok {
			return nil, fmt.Errorf("plot %q: unknown testbed %q", p.Title, p.Testbed)
		}

		uuids := map[string]bool{}
		for _, glob := range p.Benchmarks {
			matched := 0
			for _, name := range allNames {
				ok, err := path.Match(glob, name)
				if err != nil {
					return nil, fmt.Errorf("plot %q: bad benchmark glob %q: %w", p.Title, glob, err)
				}
				if ok {
					uuids[nm.Benchmarks[name]] = true
					matched++
				}
			}
			if matched == 0 {
				return nil, fmt.Errorf("plot %q: benchmark glob %q matched 0 benchmarks; candidates: %s",
					p.Title, glob, strings.Join(allNames, ", "))
			}
		}

		out = append(out, ResolvedPlot{
			Title:         p.Title,
			Measure:       measure,
			Benchmarks:    sortedKeys(uuids),
			Branch:        branch,
			Testbed:       testbed,
			Window:        p.Window,
			XAxis:         p.XAxis,
			LowerValue:    p.LowerValue,
			UpperValue:    p.UpperValue,
			LowerBoundary: p.LowerBoundary,
			UpperBoundary: p.UpperBoundary,
		})
	}
	return out, nil
}

// Plot is a live plot as returned by `bencher plot list/view`, in UUID space.
type Plot struct {
	UUID          string   `json:"uuid"`
	Title         string   `json:"title"`
	Measures      []string `json:"measures"`
	Benchmarks    []string `json:"benchmarks"`
	Branches      []string `json:"branches"`
	Testbeds      []string `json:"testbeds"`
	Window        int64    `json:"window"`
	XAxis         string   `json:"x_axis"`
	LowerValue    bool     `json:"lower_value"`
	UpperValue    bool     `json:"upper_value"`
	LowerBoundary bool     `json:"lower_boundary"`
	UpperBoundary bool     `json:"upper_boundary"`
}

// ActionKind is what to do with one plot to converge toward the spec.
type ActionKind int

const (
	// Noop: content and window match. The executor still pins the index from
	// spec order (the GET API never returns index, so it can't be diffed —
	// it's enforced unconditionally for every kept plot).
	Noop ActionKind = iota
	// UpdateMeta: content matches but window differs — a `plot update`.
	UpdateMeta
	// Recreate: benchmarks/measure/branch/testbed/x_axis/flags differ. Since
	// `plot update` can't touch those, create new then delete old.
	Recreate
	// Create: no live plot with this title.
	Create
	// Delete: a live plot whose title is not in the spec (only when prune=true).
	Delete
)

func (k ActionKind) String() string {
	switch k {
	case Noop:
		return "NOOP"
	case UpdateMeta:
		return "UPDATE"
	case Recreate:
		return "RECREATE"
	case Create:
		return "CREATE"
	case Delete:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

// Action is one unit of convergence work.
type Action struct {
	Kind    ActionKind
	Title   string
	Index   int           // target dashboard index (spec order); -1 for Delete
	Desired *ResolvedPlot // nil for Delete
	OldUUID string        // live plot to update/recreate-from/delete; "" for Create
	Reason  string        // human explanation for the dry-run plan
}

// Plan diffs desired plots (spec order) against the live plots and returns the
// actions to converge. Matching is by title. With prune, live plots whose title
// is absent from the spec are scheduled for deletion; otherwise they are left
// untouched (so ad-hoc plots made in the web UI survive).
func Plan(desired []ResolvedPlot, actual []Plot, prune bool) []Action {
	byTitle := make(map[string]Plot, len(actual))
	for _, a := range actual {
		byTitle[a.Title] = a
	}

	var actions []Action
	wanted := map[string]bool{}
	for i, d := range desired {
		wanted[d.Title] = true
		a, ok := byTitle[d.Title]
		switch {
		case !ok:
			actions = append(actions, Action{Kind: Create, Title: d.Title, Index: i, Desired: &d, Reason: "no live plot with this title"})
		default:
			if reason, differs := contentDiffers(d, a); differs {
				actions = append(actions, Action{Kind: Recreate, Title: d.Title, Index: i, Desired: &d, OldUUID: a.UUID, Reason: reason})
			} else if d.Window != a.Window {
				actions = append(actions, Action{Kind: UpdateMeta, Title: d.Title, Index: i, Desired: &d, OldUUID: a.UUID, Reason: fmt.Sprintf("window %d->%d", a.Window, d.Window)})
			} else {
				actions = append(actions, Action{Kind: Noop, Title: d.Title, Index: i, Desired: &d, OldUUID: a.UUID, Reason: "matches"})
			}
		}
	}

	if prune {
		for _, a := range actual {
			if !wanted[a.Title] {
				actions = append(actions, Action{Kind: Delete, Title: a.Title, Index: -1, OldUUID: a.UUID, Reason: "not in spec (prune)"})
			}
		}
	}
	return actions
}

// contentDiffers reports the first dimension `plot update` can't change that
// differs between desired and live, and whether any did.
func contentDiffers(d ResolvedPlot, a Plot) (string, bool) {
	switch {
	case !equalSet([]string{d.Measure}, a.Measures):
		return "measure changed", true
	case !equalSet(d.Benchmarks, a.Benchmarks):
		return fmt.Sprintf("benchmark set changed (%d->%d)", len(a.Benchmarks), len(d.Benchmarks)), true
	case !equalSet([]string{d.Branch}, a.Branches):
		return "branch changed", true
	case !equalSet([]string{d.Testbed}, a.Testbeds):
		return "testbed changed", true
	case d.XAxis != a.XAxis:
		return fmt.Sprintf("x_axis %q->%q", a.XAxis, d.XAxis), true
	case d.LowerValue != a.LowerValue || d.UpperValue != a.UpperValue ||
		d.LowerBoundary != a.LowerBoundary || d.UpperBoundary != a.UpperBoundary:
		return "boundary/value flags changed", true
	}
	return "", false
}

func equalSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int, len(a))
	for _, x := range a {
		m[x]++
	}
	for _, x := range b {
		m[x]--
	}
	for _, v := range m {
		if v != 0 {
			return false
		}
	}
	return true
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func keys(m map[string]string) string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}
