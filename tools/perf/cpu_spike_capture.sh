#!/usr/bin/env bash
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Continuously sample one process cheaply and collect a short perf trace when
# its CPU use jumps above a configured number of logical cores. This is for
# transient client CPU spikes; it deliberately records only event-local
# diagnostics instead of copying large application log directories.

set -euo pipefail

readonly DEFAULT_INTERVAL_MS=100
readonly DEFAULT_CONSECUTIVE_SAMPLES=2
readonly DEFAULT_THRESHOLD_CORES=8
readonly DEFAULT_CAPTURE_SECONDS=4
readonly DEFAULT_MAX_EVENTS=3
readonly PERF_FREQUENCY_HZ=199
readonly SAMPLE_HEADER='realtime_ns,monotonic_ns,cpu_cores,utime_ticks,stime_ticks'

pid=''
output_dir=''
interval_ms="$DEFAULT_INTERVAL_MS"
consecutive_samples="$DEFAULT_CONSECUTIVE_SAMPLES"
threshold_cores="$DEFAULT_THRESHOLD_CORES"
capture_seconds="$DEFAULT_CAPTURE_SECONDS"
max_events="$DEFAULT_MAX_EVENTS"
snapshot_command=''

usage() {
    cat <<'EOF'
Usage:
  cpu_spike_capture.sh --pid PID --output DIR [options]

Required:
  --pid PID                       Target client process PID.
  --output DIR                    Directory for samples and event evidence.

Trigger options:
  --threshold-cores N             CPU-core threshold; default 8.
  --interval-ms N                 /proc sampling interval; default 100.
  --consecutive-samples N         Consecutive samples above threshold; default 2.
  --capture-seconds N             perf capture duration after trigger; default 4.
  --max-events N                  Stop after this many captures; default 3.

Optional correlation:
  --snapshot-command COMMAND      Command evaluated once per trigger. Use it to
                                  query the load generator's current QPS, SDK
                                  counters, or the narrow UB/admission metrics.

Examples:
  sudo tools/perf/cpu_spike_capture.sh --pid 12345 --output /var/tmp/client-cpu \
    --threshold-cores 8 --snapshot-command 'curl -fsS http://127.0.0.1:8080/metrics'

The target process and perf must be visible to the invoking user. Run with the
least privilege that permits perf_event access; do not lower kernel perf_event
permissions system-wide merely for this tool.
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

is_positive_integer() {
    [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pid) pid="${2:-}"; shift 2 ;;
        --output) output_dir="${2:-}"; shift 2 ;;
        --threshold-cores) threshold_cores="${2:-}"; shift 2 ;;
        --interval-ms) interval_ms="${2:-}"; shift 2 ;;
        --consecutive-samples) consecutive_samples="${2:-}"; shift 2 ;;
        --capture-seconds) capture_seconds="${2:-}"; shift 2 ;;
        --max-events) max_events="${2:-}"; shift 2 ;;
        --snapshot-command) snapshot_command="${2:-}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) die "unknown argument: $1" ;;
    esac
done

[[ "$pid" =~ ^[1-9][0-9]*$ ]] || die '--pid must be a positive PID'
[[ -n "$output_dir" ]] || die '--output is required'
is_positive_integer "$interval_ms" || die '--interval-ms must be a positive integer'
is_positive_integer "$consecutive_samples" || die '--consecutive-samples must be a positive integer'
is_positive_integer "$capture_seconds" || die '--capture-seconds must be a positive integer'
is_positive_integer "$max_events" || die '--max-events must be a positive integer'
awk -v value="$threshold_cores" 'BEGIN { exit !(value > 0) }' || die '--threshold-cores must be positive'
[[ -r "/proc/$pid/stat" ]] || die "cannot read /proc/$pid/stat"
command -v perf >/dev/null || die 'perf is not installed or not in PATH'

mkdir -p "$output_dir"
samples_file="$output_dir/cpu_samples.csv"
events_file="$output_dir/events.csv"
metadata_file="$output_dir/metadata.txt"

if [[ ! -f "$samples_file" ]]; then
    printf '%s\n' "$SAMPLE_HEADER" > "$samples_file"
fi
if [[ ! -f "$events_file" ]]; then
    printf '%s\n' 'event_id,realtime_ns,peak_cpu_cores,perf_exit_code,event_dir' > "$events_file"
fi

{
    printf 'pid=%s\n' "$pid"
    printf 'started_at=%s\n' "$(date --iso-8601=seconds)"
    printf 'threshold_cores=%s\ninterval_ms=%s\nconsecutive_samples=%s\n' \
        "$threshold_cores" "$interval_ms" "$consecutive_samples"
    printf 'perf_frequency_hz=%s\ncapture_seconds=%s\n' "$PERF_FREQUENCY_HZ" "$capture_seconds"
    printf 'command='
    tr '\0' ' ' < "/proc/$pid/cmdline" || true
    printf '\n'
} > "$metadata_file"

read_cpu_ticks() {
    # comm is wrapped in parentheses and may contain spaces, so parse only the
    # suffix after its final ')'. In that suffix fields 12/13 are utime/stime.
    local stat_line suffix
    stat_line=$(<"/proc/$pid/stat") || return 1
    suffix=${stat_line##*) }
    awk '{ print $12, $13 }' <<<"$suffix"
}

read_monotonic_ns() {
    # /proc/uptime is CLOCK_MONOTONIC-derived on Linux. Its centisecond
    # precision is sufficient for the default 100ms sampling interval and is
    # not affected by wall-clock/NTP adjustments.
    awk '{ printf "%.0f", $1 * 1000000000 }' /proc/uptime
}

capture_event() {
    local event_id="$1"
    local observed_cores="$2"
    local realtime_ns
    realtime_ns=$(date +%s%N)
    local event_dir="$output_dir/event_${event_id}_${realtime_ns}"
    mkdir -p "$event_dir"

    {
        printf 'trigger_realtime_ns=%s\n' "$realtime_ns"
        printf 'trigger_cpu_cores=%s\n' "$observed_cores"
        printf 'pid=%s\n' "$pid"
    } > "$event_dir/trigger.txt"
    tail -n 80 "$samples_file" > "$event_dir/cpu_samples_before_trigger.csv"
    cp "/proc/$pid/status" "$event_dir/proc_status.txt" 2>/dev/null || true
    ps -L -p "$pid" -o pid,tid,psr,pcpu,stat,comm > "$event_dir/threads.txt" 2>/dev/null || true
    ss -tinp > "$event_dir/sockets.txt" 2>/dev/null || true
    if [[ -n "$snapshot_command" ]]; then
        bash -c "$snapshot_command" > "$event_dir/correlation_snapshot.txt" 2>&1 || true
    fi

    set +e
    perf record -F "$PERF_FREQUENCY_HZ" -g --call-graph dwarf,16384 -p "$pid" \
        -o "$event_dir/perf.data" -- sleep "$capture_seconds" > "$event_dir/perf_record.log" 2>&1
    local perf_rc=$?
    set -e
    if [[ -f "$event_dir/perf.data" ]]; then
        perf script -i "$event_dir/perf.data" > "$event_dir/perf.script" 2> "$event_dir/perf_script.log" || true
    fi
    if command -v stackcollapse-perf.pl >/dev/null && command -v flamegraph.pl >/dev/null \
        && [[ -s "$event_dir/perf.script" ]]; then
        stackcollapse-perf.pl "$event_dir/perf.script" > "$event_dir/perf.folded" || true
        flamegraph.pl --title "CPU spike event $event_id" "$event_dir/perf.folded" > "$event_dir/flamegraph.svg" || true
    fi
    tail -n 120 "$samples_file" > "$event_dir/cpu_samples_after_trigger.csv"
    printf '%s,%s,%s,%s,%s\n' "$event_id" "$realtime_ns" "$observed_cores" "$perf_rc" "$event_dir" >> "$events_file"
    echo "captured event $event_id in $event_dir" >&2
}

readonly ticks_per_second=$(getconf CLK_TCK)
previous_ticks=''
previous_monotonic_ns=''
above_count=0
event_count=0

while [[ -r "/proc/$pid/stat" && "$event_count" -lt "$max_events" ]]; do
    read -r utime_ticks stime_ticks < <(read_cpu_ticks) || break
    current_ticks=$((utime_ticks + stime_ticks))
    current_monotonic_ns=$(read_monotonic_ns)
    if [[ -n "$previous_ticks" ]]; then
        delta_ticks=$((current_ticks - previous_ticks))
        delta_ns=$((current_monotonic_ns - previous_monotonic_ns))
        if (( delta_ticks >= 0 && delta_ns > 0 )); then
            cpu_cores=$(awk -v ticks="$delta_ticks" -v hz="$ticks_per_second" -v ns="$delta_ns" \
                'BEGIN { printf "%.3f", ticks * 1000000000 / (hz * ns) }')
            printf '%s,%s,%s,%s,%s\n' "$(date +%s%N)" "$current_monotonic_ns" "$cpu_cores" \
                "$utime_ticks" "$stime_ticks" >> "$samples_file"
            if awk -v measured="$cpu_cores" -v threshold="$threshold_cores" \
                'BEGIN { exit !(measured >= threshold) }'; then
                above_count=$((above_count + 1))
            else
                above_count=0
            fi
            if (( above_count >= consecutive_samples )); then
                event_count=$((event_count + 1))
                capture_event "$event_count" "$cpu_cores"
                above_count=0
            fi
        fi
    fi
    previous_ticks="$current_ticks"
    previous_monotonic_ns="$current_monotonic_ns"
    sleep_seconds=$(awk -v milliseconds="$interval_ms" 'BEGIN { printf "%.3f", milliseconds / 1000 }')
    sleep "$sleep_seconds"
done

echo "stopped after $event_count captured event(s); samples: $samples_file" >&2
