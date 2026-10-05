# Invalid runs, kept for the record

`exp4_amplification_fault_not_applied.jsonl`: the dependency fault (400 ms on
the rate cache) was injected through Toxiproxy, but the benchmark image reads
its baked `/workspace/config.json`, not the mounted `/config.json`, so the
services never connected through Toxiproxy and the fault did nothing (BUG_LOG
B3). These runs measured an unfaulted system. exp4 was rerun with `tc netem`.
