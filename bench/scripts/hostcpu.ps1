# Samples whole-host CPU (Windows view, all 4 cores) every 2 s into
# build/host_cpu.log as "<unix seconds> <percent>". The WSL VM saturating its
# 2 vCPUs shows as about 55-65%; more than that means something outside the
# benchmark is using the host. lcbench.py attaches the samples for each run.
$log = Join-Path $PSScriptRoot '..\..\build\host_cpu.log'
Get-Counter '\Processor(_Total)\% Processor Time' -SampleInterval 2 -Continuous | ForEach-Object {
  "{0} {1:F1}" -f [DateTimeOffset]::Now.ToUnixTimeSeconds(), $_.CounterSamples[0].CookedValue | Out-File -Append -Encoding ascii $log
}
