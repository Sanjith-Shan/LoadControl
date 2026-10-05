// Command lcsim runs the hotelReservation overload simulator.
//
//	lcsim run       -config LIMIT=gradient2,DEADLINE=on,load.x=3 [-params f] [-out f.jsonl]
//	lcsim sweep     -param load.x -values 0.5,1,2,3,4 [-config ...] -out f.jsonl
//	lcsim calibrate -capacity 1450 [-lat search=12,recommend=8,user=3,reserve=10] -out p.json
//	lcsim replay    -results results/exp0_capacity.jsonl [-results ...] -out results/sim_replay.jsonl
//
// run and sweep write JSON lines: one per simulated second (run, or sweep
// with -seconds) and one summary per run.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/Sanjith-Shan/LoadControl/sim"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = run(os.Args[2:])
	case "sweep":
		err = sweep(os.Args[2:])
	case "calibrate":
		err = calibrate(os.Args[2:])
	case "replay":
		err = replay(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "lcsim:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: lcsim run|sweep|calibrate|replay [flags] (-h for flags)")
	os.Exit(2)
}

func output(path string) (io.Writer, func() error, error) {
	if path == "" || path == "-" {
		return os.Stdout, func() error { return nil }, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	return f, f.Close, nil
}

func params(path, config string) (*sim.Params, error) {
	p, err := sim.Load(path)
	if err != nil {
		return nil, err
	}
	return p, p.ApplyConfig(config)
}

func write(enc *json.Encoder, label string, r *sim.Result, seconds bool) error {
	if seconds {
		for _, s := range r.Seconds {
			s.Run = label
			if err := enc.Encode(s); err != nil {
				return err
			}
		}
	}
	r.Summary.Run = label
	return enc.Encode(r.Summary)
}

func run(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	pf := fs.String("params", "", "params file (default: built-in sim/params.json)")
	cfg := fs.String("config", "", "overrides: LoadControl keys (LIMIT=gradient2) and params paths (load.x=3)")
	out := fs.String("out", "", "JSONL output (default stdout)")
	label := fs.String("label", "", "run label (default: the -config string)")
	summary := fs.Bool("summary", false, "write only the summary line")
	fs.Parse(args)
	p, err := params(*pf, *cfg)
	if err != nil {
		return err
	}
	r, err := sim.Run(p)
	if err != nil {
		return err
	}
	w, closeFn, err := output(*out)
	if err != nil {
		return err
	}
	if *label == "" {
		*label = *cfg
	}
	if err := write(json.NewEncoder(w), *label, r, !*summary); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "goodput %.0f rps (%.2fx capacity) of %.0f offered, success %.3f, %d events in %.0f ms\n",
		r.Summary.GoodputRPS, r.Summary.GoodputX, r.Summary.OfferedRPS, r.Summary.SuccessRate, r.Summary.Events, r.Summary.WallMS)
	return closeFn()
}

func sweep(args []string) error {
	fs := flag.NewFlagSet("sweep", flag.ExitOnError)
	pf := fs.String("params", "", "params file")
	cfg := fs.String("config", "", "base overrides")
	param := fs.String("param", "load.x", "key to vary (params path or LoadControl key)")
	values := fs.String("values", "0.5,1,1.5,2,2.5,3,3.5,4", "values; separate with ';' if a value contains commas")
	configs := fs.String("configs", "", "instead of -param: whole override sets separated by ';' (each run labeled by its set)")
	seeds := fs.Int("seeds", 1, "repeat each point with seeds seed..seed+n-1")
	out := fs.String("out", "", "JSONL output")
	seconds := fs.Bool("seconds", false, "also write per-second lines")
	par := fs.Int("parallel", runtime.NumCPU(), "runs in parallel")
	fs.Parse(args)
	base, err := params(*pf, *cfg)
	if err != nil {
		return err
	}
	type job struct {
		label string
		p     *sim.Params
	}
	var jobs []job
	add := func(label string, from *sim.Params, set string) error {
		for k := range *seeds {
			p := from.Clone()
			if err := p.ApplyConfig(set); err != nil {
				return err
			}
			p.Seed += uint64(k)
			l := label
			if *seeds > 1 {
				l += " seed=" + strconv.FormatUint(p.Seed, 10)
			}
			jobs = append(jobs, job{l, p})
		}
		return nil
	}
	if *configs != "" {
		for _, set := range strings.Split(*configs, ";") {
			if err := add(strings.TrimSpace(set), base, set); err != nil {
				return err
			}
		}
	} else {
		sep := ","
		if strings.Contains(*values, ";") {
			sep = ";"
		}
		for _, v := range strings.Split(*values, sep) {
			v = strings.TrimSpace(v)
			p := base.Clone()
			if err := p.Set(*param, v); err != nil {
				return err
			}
			if err := add(*param+"="+v, p, ""); err != nil {
				return err
			}
		}
	}
	results := make([]*sim.Result, len(jobs))
	errs := make([]error, len(jobs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, max(1, *par))
	for i, j := range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i], errs[i] = sim.Run(j.p)
		}()
	}
	wg.Wait()
	w, closeFn, err := output(*out)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	for i, j := range jobs {
		if errs[i] != nil {
			return fmt.Errorf("%s: %v", j.label, errs[i])
		}
		if err := write(enc, j.label, results[i], *seconds); err != nil {
			return err
		}
		s := results[i].Summary
		fmt.Fprintf(os.Stderr, "%-40s offered %7.0f goodput %7.0f (%.2fx) success %.3f\n", j.label, s.OfferedRPS, s.GoodputRPS, s.GoodputX, s.SuccessRate)
	}
	return closeFn()
}

func calibrate(args []string) error {
	fs := flag.NewFlagSet("calibrate", flag.ExitOnError)
	pf := fs.String("params", "", "params file to start from")
	cfg := fs.String("config", "", "overrides describing the measured no-control run (env, user timeout, mix)")
	capacity := fs.Float64("capacity", 0, "measured max goodput (rps) with no control (required)")
	lat := fs.String("lat", "", "measured low-load mean latency per request type in ms: search=..,recommend=..,user=..,reserve=..")
	lowRPS := fs.Float64("lowload-rps", 50, "rate the low-load latencies were measured at")
	hidden := fs.Float64("hidden-ms", -1, "fix hidden_ms (CPU per user attempt outside the handlers) instead of fitting it; -1 = fit")
	out := fs.String("out", "", "write the calibrated params here (default stdout)")
	fs.Parse(args)
	if *capacity <= 0 {
		return fmt.Errorf("-capacity is required")
	}
	p, err := params(*pf, *cfg)
	if err != nil {
		return err
	}
	in := sim.Measured{CapacityRPS: *capacity, LowLoadRPS: *lowRPS, LatencyMS: map[string]float64{}, HiddenMS: *hidden}
	for _, kv := range sim.SplitConfig(*lat) {
		x, err := strconv.ParseFloat(kv[1], 64)
		if err != nil {
			return fmt.Errorf("-lat %s: %v", kv[0], err)
		}
		in.LatencyMS[kv[0]] = x
	}
	q, report, err := sim.Calibrate(p, in, func(msg string) { fmt.Fprintln(os.Stderr, msg) })
	if err != nil {
		return err
	}
	w, closeFn, err := output(*out)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(q); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, report)
	return closeFn()
}

type multi []string

func (m *multi) String() string     { return strings.Join(*m, ",") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

// replay simulates every recorded run in the given result files with one
// parameter set and writes measured against simulated, one line per run.
func replay(args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	pf := fs.String("params", "", "params file (default: built-in, calibrated)")
	cfg := fs.String("config", "", "overrides applied to every run (for experiments with the model)")
	var files multi
	fs.Var(&files, "results", "results JSONL file from bench/lcbench.py (repeatable; positional args too)")
	skip := fs.String("skip", "sim_replay", "skip files whose name contains this")
	out := fs.String("out", "", "JSONL output")
	par := fs.Int("parallel", 1, "runs in parallel")
	secOut := fs.String("seconds", "", "also write the simulated per-second series of every run here (JSONL)")
	exclude := fs.String("exclude", "", "comma list of runs to treat as contaminated: exp/name, or exp/name@time-prefix for one occurrence")
	fs.Parse(args)
	files = append(files, fs.Args()...)
	base, err := params(*pf, *cfg)
	if err != nil {
		return err
	}
	type job struct {
		file string
		rec  *sim.Record
	}
	var jobs []job
	for _, f := range files {
		if *skip != "" && strings.Contains(f, *skip) {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(b), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			r := &sim.Record{}
			if err := json.Unmarshal([]byte(line), r); err != nil {
				return fmt.Errorf("%s:%d: %v", f, i+1, err)
			}
			if r.DurationS <= 0 {
				continue
			}
			jobs = append(jobs, job{filepath.ToSlash(f), r})
		}
	}
	res := make([]*sim.Comparison, len(jobs))
	series := make([]*sim.Result, len(jobs))
	errs := make([]error, len(jobs))
	var wg sync.WaitGroup
	sem := make(chan struct{}, max(1, *par))
	for i, j := range jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			res[i], series[i], errs[i] = sim.Replay(base, j.rec, j.file)
			if *secOut == "" {
				series[i] = nil
			}
			if errs[i] == nil && res[i].Contaminated == "" && excluded(*exclude, j.rec) {
				res[i].Contaminated = "listed in -exclude"
			}
		}()
	}
	wg.Wait()
	w, closeFn, err := output(*out)
	if err != nil {
		return err
	}
	if *secOut != "" {
		sw, sclose, err := output(*secOut)
		if err != nil {
			return err
		}
		se := json.NewEncoder(sw)
		for i, r := range series {
			if r == nil {
				continue
			}
			for _, x := range r.Seconds {
				x.Run = jobs[i].rec.Exp + "/" + jobs[i].rec.Name + "@" + jobs[i].rec.Time
				se.Encode(x)
			}
		}
		if err := sclose(); err != nil {
			return err
		}
	}
	enc := json.NewEncoder(w)
	byExp := map[string][]*sim.Comparison{}
	var exps []string
	for i, c := range res {
		if errs[i] != nil {
			return fmt.Errorf("%s %s: %v", jobs[i].file, jobs[i].rec.Name, errs[i])
		}
		if err := enc.Encode(c); err != nil {
			return err
		}
		if byExp[c.Exp] == nil {
			exps = append(exps, c.Exp)
		}
		byExp[c.Exp] = append(byExp[c.Exp], c)
		rel := "-"
		if c.RelErr != nil {
			rel = fmt.Sprintf("%.0f%%", 100**c.RelErr)
		}
		fmt.Fprintf(os.Stderr, "%-10s %-32s good/s measured %7.1f sim %7.1f  err %+7.1f (%s)%s\n",
			c.Exp, c.Name, c.Measured.GoodRPS, c.Sim.GoodRPS, c.AbsErr, rel, recov(c))
	}
	fmt.Fprintln(os.Stderr, "\nexperiment  runs  median |err| good/s  median rel  max rel  within 10%")
	for _, e := range exps {
		var abs, rel []float64
		in := 0
		n := 0
		for _, c := range byExp[e] {
			if c.Contaminated != "" {
				continue
			}
			n++
			abs = append(abs, math.Abs(c.AbsErr))
			if c.RelErr != nil {
				rel = append(rel, *c.RelErr)
				if *c.RelErr <= 0.1 {
					in++
				}
			}
		}
		slices.Sort(abs)
		slices.Sort(rel)
		med := func(x []float64) float64 {
			if len(x) == 0 {
				return math.NaN()
			}
			return x[len(x)/2]
		}
		fmt.Fprintf(os.Stderr, "%-10s %5d  %18.1f  %9.0f%%  %6.0f%%  %4d/%d\n", e, n, med(abs), 100*med(rel),
			100*slices.Max(append(rel, 0)), in, len(rel))
	}
	return closeFn()
}

func recov(c *sim.Comparison) string {
	if c.Faults == 0 {
		return ""
	}
	f := func(p *int) string {
		if p == nil {
			return "never"
		}
		return strconv.Itoa(*p)
	}
	return fmt.Sprintf("  recovery s measured %s sim %s", f(c.Measured.RecoveryS), f(c.Sim.RecoveryS))
}

// excluded reports whether r is named in the -exclude list.
func excluded(list string, r *sim.Record) bool {
	id := r.Exp + "/" + r.Name
	for _, e := range strings.Split(list, ",") {
		if e = strings.TrimSpace(e); e != "" && (e == id || strings.HasPrefix(id+"@"+r.Time, e) && strings.Contains(e, "@")) {
			return true
		}
	}
	return false
}
