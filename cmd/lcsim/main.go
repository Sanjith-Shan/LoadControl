// Command lcsim runs the hotelReservation overload simulator.
//
//	lcsim run       -config LIMIT=gradient2,DEADLINE=on,load.x=3 [-params f] [-out f.jsonl]
//	lcsim sweep     -param load.x -values 0.5,1,2,3,4 [-config ...] -out f.jsonl
//	lcsim calibrate -capacity 1450 [-lat search=12,recommend=8,user=3,reserve=10] -out p.json
//
// run and sweep write JSON lines: one per simulated second (run, or sweep
// with -seconds) and one summary per run.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
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
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "lcsim:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: lcsim run|sweep|calibrate [flags] (-h for flags)")
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
	out := fs.String("out", "", "write the calibrated params here (default stdout)")
	fs.Parse(args)
	if *capacity <= 0 {
		return fmt.Errorf("-capacity is required")
	}
	p, err := params(*pf, *cfg)
	if err != nil {
		return err
	}
	in := sim.Measured{CapacityRPS: *capacity, LowLoadRPS: *lowRPS, LatencyMS: map[string]float64{}}
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
