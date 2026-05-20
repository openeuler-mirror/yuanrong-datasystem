#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BINARY="$SCRIPT_DIR/build/kvtest"
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

PASS=0; FAIL=0
check() {
    local name="$1" config="$2" expected="$3"
    OUT=$($BINARY $config 2>&1 || true)
    LINE=$(echo "$OUT" | grep "CPU affinity:" || true)
    if echo "$LINE" | grep -q "$expected"; then
        echo "PASS: $name"; PASS=$((PASS+1))
    else
        echo "FAIL: $name -> got '$LINE', expected '$expected'"; FAIL=$((FAIL+1))
    fi
}

echo "=== Test 1: Auto-detect (no cpu_affinity) ==="
echo '{"instance_id":0,"etcd_address":"127.0.0.1:2379","cluster_name":"test"}' > $TMPDIR/1.json
check "auto-detect" $TMPDIR/1.json "CPUs"

echo "=== Test 2: Manual cpu_affinity='0-3' ==="
echo '{"instance_id":0,"etcd_address":"127.0.0.1:2379","cluster_name":"test","cpu_affinity":"0-3"}' > $TMPDIR/2.json
check "manual 0-3" $TMPDIR/2.json "4 CPUs \[0,1,2,3\]"

echo "=== Test 3: taskset constraint ==="
OUT=$(taskset -c 0-3 $BINARY $TMPDIR/1.json 2>&1 || true)
LINE=$(echo "$OUT" | grep "CPU affinity:" || true)
echo "$LINE" | grep -q "4 CPUs \[0,1,2,3\]" && { echo "PASS: taskset"; PASS=$((PASS+1)); } || { echo "FAIL: taskset"; FAIL=$((FAIL+1)); }

echo "=== Test 4: cpu_affinity='2-5' ==="
echo '{"instance_id":0,"etcd_address":"127.0.0.1:2379","cluster_name":"test","cpu_affinity":"2-5"}' > $TMPDIR/4.json
check "2-5" $TMPDIR/4.json "4 CPUs \[2,3,4,5\]"

echo "=== Test 5: Invalid 'abc' (fallback) ==="
echo '{"instance_id":0,"etcd_address":"127.0.0.1:2379","cluster_name":"test","cpu_affinity":"abc"}' > $TMPDIR/5.json
OUT=$($BINARY $TMPDIR/5.json 2>&1 || true)
LINE=$(echo "$OUT" | grep "CPU affinity:" || true)
[ -n "$LINE" ] && { echo "PASS: no crash, fallback -> $LINE"; PASS=$((PASS+1)); } || { echo "FAIL: crashed"; echo "$OUT" | head -5; FAIL=$((FAIL+1)); }

echo "=== Test 6: Mixed '0,2-4,7' ==="
echo '{"instance_id":0,"etcd_address":"127.0.0.1:2379","cluster_name":"test","cpu_affinity":"0,2-4,7"}' > $TMPDIR/6.json
check "mixed" $TMPDIR/6.json "5 CPUs \[0,2,3,4,7\]"

echo "=== Test 7: Single CPU '3' ==="
echo '{"instance_id":0,"etcd_address":"127.0.0.1:2379","cluster_name":"test","cpu_affinity":"3"}' > $TMPDIR/7.json
check "single" $TMPDIR/7.json "1 CPUs \[3\]"

echo "=== Test 8: Reversed range '7-3' (auto-swap) ==="
echo '{"instance_id":0,"etcd_address":"127.0.0.1:2379","cluster_name":"test","cpu_affinity":"7-3"}' > $TMPDIR/8.json
check "reversed" $TMPDIR/8.json "5 CPUs \[3,4,5,6,7\]"

echo "=== Test 9: Negative number '-2' (filtered) ==="
echo '{"instance_id":0,"etcd_address":"127.0.0.1:2379","cluster_name":"test","cpu_affinity":"-2,0,3"}' > $TMPDIR/9.json
check "negative" $TMPDIR/9.json "2 CPUs \[0,3\]"

echo "=== Test 10: Out-of-range '0-99999' (capped by CPU_SETSIZE) ==="
echo '{"instance_id":0,"etcd_address":"127.0.0.1:2379","cluster_name":"test","cpu_affinity":"0-99999"}' > $TMPDIR/10.json
OUT=$($BINARY $TMPDIR/10.json 2>&1 || true)
LINE=$(echo "$OUT" | grep "CPU affinity:" || true)
echo "$LINE"
echo "$OUT" | grep -q "WARNING" && echo "WARN detected (OK)" || true
# Should not crash
[ -n "$LINE" ] && { echo "PASS: no crash on large range"; PASS=$((PASS+1)); } || { echo "FAIL: crashed"; FAIL=$((FAIL+1)); }

echo "=== Test 11: Trailing comma '0,2,' ==="
echo '{"instance_id":0,"etcd_address":"127.0.0.1:2379","cluster_name":"test","cpu_affinity":"0,2,"}' > $TMPDIR/11.json
check "trailing comma" $TMPDIR/11.json "2 CPUs \[0,2\]"

echo "=== Test 12: Empty string (auto-detect fallback) ==="
echo '{"instance_id":0,"etcd_address":"127.0.0.1:2379","cluster_name":"test","cpu_affinity":""}' > $TMPDIR/12.json
OUT=$($BINARY $TMPDIR/12.json 2>&1 || true)
LINE=$(echo "$OUT" | grep "CPU affinity:" || true)
echo "$LINE" | grep -q "CPUs" && { echo "PASS: empty string fallback"; PASS=$((PASS+1)); } || { echo "FAIL"; FAIL=$((FAIL+1)); }

echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
