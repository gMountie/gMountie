package bmf

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type PlotSyncSuite struct {
	suite.Suite
}

func TestPlotSyncSuite(t *testing.T) {
	suite.Run(t, new(PlotSyncSuite))
}

const sampleSpec = `
defaults:
  branch: master
  testbed: gmountie-perf-pod
  window: 315360000
  x_axis: date_time
plots:
  - title: Sequential throughput (LAN)
    measure: throughput
    benchmarks: ["Seq*MiB/lan"]
  - title: Random 4KiB I/O latency
    measure: latency
    benchmarks: ["Random*4KiB/*"]
    window: 999
`

func (s *PlotSyncSuite) TestLoadAndNormalizeAppliesDefaults() {
	sp, err := LoadSpec(strings.NewReader(sampleSpec))
	s.Require().NoError(err)
	plots, err := sp.Normalize()
	s.Require().NoError(err)
	s.Require().Len(plots, 2)

	// Defaults filled where the plot left them empty.
	s.Equal("master", plots[0].Branch)
	s.Equal("gmountie-perf-pod", plots[0].Testbed)
	s.Equal(int64(315360000), plots[0].Window)
	s.Equal("date_time", plots[0].XAxis)

	// Per-plot override wins over the default.
	s.Equal(int64(999), plots[1].Window)
}

func (s *PlotSyncSuite) TestLoadRejectsUnknownKey() {
	_, err := LoadSpec(strings.NewReader("plots:\n  - title: x\n    measur: throughput\n"))
	s.Require().Error(err) // KnownFields(true): "measur" typo is fatal
}

func (s *PlotSyncSuite) TestNormalizeRejectsMissingFieldsAndDupes() {
	cases := map[string]string{
		"no measure":    "plots:\n  - title: A\n    benchmarks: [\"X\"]\n",
		"no benchmarks": "defaults: {branch: m, testbed: t}\nplots:\n  - title: A\n    measure: throughput\n",
		"no branch":     "plots:\n  - title: A\n    measure: throughput\n    benchmarks: [\"X\"]\n    testbed: t\n",
		"dup title":     "defaults: {branch: m, testbed: t}\nplots:\n  - {title: A, measure: throughput, benchmarks: [\"X\"]}\n  - {title: A, measure: latency, benchmarks: [\"Y\"]}\n",
	}
	for name, doc := range cases {
		sp, err := LoadSpec(strings.NewReader(doc))
		s.Require().NoError(err, name)
		_, err = sp.Normalize()
		s.Require().Error(err, name)
	}
}

func (s *PlotSyncSuite) maps() NameMaps {
	return NameMaps{
		Benchmarks: map[string]string{
			"SeqRead1MiB/lan":     "br1",
			"SeqReadOpt1MiB/lan":  "br2",
			"SeqRead1MiB/wan":     "bw1",
			"RandomRead4KiB/lan":  "rr-lan",
			"RandomWrite4KiB/wan": "rw-wan",
		},
		Measures: map[string]string{"throughput": "m-thru", "latency": "m-lat"},
		Branches: map[string]string{"master": "branch-master"},
		Testbeds: map[string]string{"gmountie-perf-pod": "tb-pod"},
	}
}

func (s *PlotSyncSuite) TestResolveExpandsGlobsAndNames() {
	plots := []PlotSpec{{
		Title: "Seq LAN", Measure: "throughput", Benchmarks: []string{"Seq*MiB/lan"},
		Branch: "master", Testbed: "gmountie-perf-pod", Window: 10, XAxis: "date_time",
	}}
	got, err := Resolve(plots, s.maps())
	s.Require().NoError(err)
	s.Require().Len(got, 1)
	// "Seq*MiB/lan" matches the two LAN seq benches; "*" must not cross "/", so
	// the WAN bench is excluded. Result is sorted+deduped.
	s.Equal([]string{"br1", "br2"}, got[0].Benchmarks)
	s.Equal("m-thru", got[0].Measure)
	s.Equal("branch-master", got[0].Branch)
	s.Equal("tb-pod", got[0].Testbed)
}

func (s *PlotSyncSuite) TestResolveDedupesOverlappingGlobs() {
	plots := []PlotSpec{{
		Title: "Rand", Measure: "latency", Benchmarks: []string{"Random*4KiB/*", "RandomRead4KiB/lan"},
		Branch: "master", Testbed: "gmountie-perf-pod",
	}}
	got, err := Resolve(plots, s.maps())
	s.Require().NoError(err)
	s.Equal([]string{"rr-lan", "rw-wan"}, got[0].Benchmarks) // rr-lan not double-counted
}

func (s *PlotSyncSuite) TestResolveZeroMatchIsError() {
	plots := []PlotSpec{{
		Title: "Nope", Measure: "throughput", Benchmarks: []string{"DoesNotExist*"},
		Branch: "master", Testbed: "gmountie-perf-pod",
	}}
	_, err := Resolve(plots, s.maps())
	s.Require().Error(err)
	s.Contains(err.Error(), "matched 0 benchmarks")
}

func (s *PlotSyncSuite) TestResolveUnknownMeasureIsError() {
	plots := []PlotSpec{{
		Title: "X", Measure: "bogus", Benchmarks: []string{"Seq*MiB/lan"},
		Branch: "master", Testbed: "gmountie-perf-pod",
	}}
	_, err := Resolve(plots, s.maps())
	s.Require().Error(err)
	s.Contains(err.Error(), "unknown measure")
}

// --- planner ---

func (s *PlotSyncSuite) desired() ResolvedPlot {
	return ResolvedPlot{
		Title: "P", Measure: "m1", Benchmarks: []string{"a", "b"},
		Branch: "br", Testbed: "tb", Window: 100, XAxis: "date_time",
	}
}

func (s *PlotSyncSuite) live(d ResolvedPlot) Plot {
	return Plot{
		UUID: "old", Title: d.Title, Measures: []string{d.Measure},
		Benchmarks: append([]string(nil), d.Benchmarks...),
		Branches:   []string{d.Branch}, Testbeds: []string{d.Testbed},
		Window: d.Window, XAxis: d.XAxis,
	}
}

func (s *PlotSyncSuite) TestPlanNoopWhenEverythingMatches() {
	d := s.desired()
	actions := Plan([]ResolvedPlot{d}, []Plot{s.live(d)}, false)
	s.Require().Len(actions, 1)
	s.Equal(Noop, actions[0].Kind)
	s.Equal(0, actions[0].Index) // index carried for unconditional enforcement
}

func (s *PlotSyncSuite) TestPlanBenchSetOrderIndependent() {
	d := s.desired()
	a := s.live(d)
	a.Benchmarks = []string{"b", "a"} // same set, different order -> still NOOP
	actions := Plan([]ResolvedPlot{d}, []Plot{a}, false)
	s.Equal(Noop, actions[0].Kind)
}

func (s *PlotSyncSuite) TestPlanCreateWhenTitleAbsent() {
	d := s.desired()
	actions := Plan([]ResolvedPlot{d}, nil, false)
	s.Require().Len(actions, 1)
	s.Equal(Create, actions[0].Kind)
	s.Empty(actions[0].OldUUID)
}

func (s *PlotSyncSuite) TestPlanRecreateOnBenchmarkChange() {
	d := s.desired()
	a := s.live(d)
	a.Benchmarks = []string{"a"} // a new bench was added in the spec
	actions := Plan([]ResolvedPlot{d}, []Plot{a}, false)
	s.Equal(Recreate, actions[0].Kind)
	s.Equal("old", actions[0].OldUUID)
	s.Contains(actions[0].Reason, "benchmark set changed")
}

func (s *PlotSyncSuite) TestPlanRecreateOnMeasureXAxisAndFlags() {
	d := s.desired()

	mChanged := s.live(d)
	mChanged.Measures = []string{"other"}
	s.Equal(Recreate, Plan([]ResolvedPlot{d}, []Plot{mChanged}, false)[0].Kind)

	xChanged := s.live(d)
	xChanged.XAxis = "version"
	s.Equal(Recreate, Plan([]ResolvedPlot{d}, []Plot{xChanged}, false)[0].Kind)

	fChanged := s.live(d)
	fChanged.UpperBoundary = true // desired has it false
	s.Equal(Recreate, Plan([]ResolvedPlot{d}, []Plot{fChanged}, false)[0].Kind)
}

func (s *PlotSyncSuite) TestPlanUpdateMetaOnWindowOnly() {
	d := s.desired()
	a := s.live(d)
	a.Window = 50 // content identical, only window differs
	actions := Plan([]ResolvedPlot{d}, []Plot{a}, false)
	s.Require().Len(actions, 1)
	s.Equal(UpdateMeta, actions[0].Kind)
	s.Equal("old", actions[0].OldUUID)
}

func (s *PlotSyncSuite) TestPlanPruneDeletesUntrackedOnlyWhenEnabled() {
	d := s.desired()
	stray := Plot{UUID: "stray", Title: "Ad-hoc"}

	// prune off: stray survives.
	off := Plan([]ResolvedPlot{d}, []Plot{s.live(d), stray}, false)
	s.Require().Len(off, 1)
	s.Equal(Noop, off[0].Kind)

	// prune on: stray is deleted, tracked plot still NOOP.
	on := Plan([]ResolvedPlot{d}, []Plot{s.live(d), stray}, true)
	s.Require().Len(on, 2)
	s.Equal(Delete, on[1].Kind)
	s.Equal("stray", on[1].OldUUID)
}

func (s *PlotSyncSuite) TestPlanAssignsIndexFromSpecOrder() {
	d1 := s.desired()
	d1.Title = "first"
	d2 := s.desired()
	d2.Title = "second"
	actions := Plan([]ResolvedPlot{d1, d2}, nil, false)
	s.Require().Len(actions, 2)
	s.Equal(0, actions[0].Index)
	s.Equal(1, actions[1].Index)
}

// --- measure planner ---

func (s *PlotSyncSuite) liveMeasures() []Measure {
	return []Measure{
		{UUID: "m-thru", Name: "Throughput", Slug: "throughput", Units: "megabytes / second (MB/s)"},
		{UUID: "m-pct", Name: "throughput_pct_of_raw", Slug: "throughput-pct-of-raw", Units: "Measure (units)"},
	}
}

func (s *PlotSyncSuite) TestPlanMeasuresNoopWhenUnitsMatch() {
	got, err := PlanMeasures(map[string]string{"throughput": "megabytes / second (MB/s)"}, s.liveMeasures())
	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal(MeasureNoop, got[0].Kind)
}

func (s *PlotSyncSuite) TestPlanMeasuresUpdatesWhenUnitsDiffer() {
	got, err := PlanMeasures(map[string]string{"throughput_pct_of_raw": "percent of raw ceiling (%)"}, s.liveMeasures())
	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal(MeasureUpdateUnits, got[0].Kind)
	s.Equal("m-pct", got[0].UUID) // resolved by name
	s.Equal("Measure (units)", got[0].From)
	s.Equal("percent of raw ceiling (%)", got[0].To)
}

func (s *PlotSyncSuite) TestPlanMeasuresResolvesBySlug() {
	// key given as the dash-slug rather than the underscore name.
	got, err := PlanMeasures(map[string]string{"throughput-pct-of-raw": "x"}, s.liveMeasures())
	s.Require().NoError(err)
	s.Equal("m-pct", got[0].UUID)
}

func (s *PlotSyncSuite) TestPlanMeasuresUnknownIsError() {
	_, err := PlanMeasures(map[string]string{"bogus": "x"}, s.liveMeasures())
	s.Require().Error(err)
	s.Contains(err.Error(), "unknown measure")
}

func (s *PlotSyncSuite) TestPlanMeasuresIsDeterministic() {
	desired := map[string]string{"throughput": "a", "throughput_pct_of_raw": "b"}
	first, err := PlanMeasures(desired, s.liveMeasures())
	s.Require().NoError(err)
	for i := 0; i < 5; i++ {
		again, err := PlanMeasures(desired, s.liveMeasures())
		s.Require().NoError(err)
		s.Equal(first, again) // sorted-key iteration -> stable order
	}
}
