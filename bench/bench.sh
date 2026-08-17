#!/usr/bin/env bash
#
# Benchmark the Scala (java -jar) and Rust (native) parquet readers side by side.
#   * wall-clock  -> hyperfine (warmup + median/stddev over N runs), if installed
#   * peak RSS + CPU -> /usr/bin/time -v (one run each)
#
# Usage: bench/bench.sh <bucket> <path> <column> <op> <value> [partCol:type,...]
# Env:   S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY  (forwarded to both apps)
#        JVM_XMX (default 1g)   RUNS (default 10)
#
# Build the artifacts first:
#   (cd scala && sbt assembly)          -> scala/target/scala-2.13/reader.jar
#   (cd rust  && cargo build --release) -> rust/target/release/parquet-seaweed-reader
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR="$ROOT/scala/target/scala-2.13/reader.jar"
BIN="$ROOT/rust/target/release/parquet-seaweed-reader"
XMX="${JVM_XMX:-1g}"
RUNS="${RUNS:-10}"

[[ -f "$JAR" ]] || { echo "Missing $JAR — run: (cd scala && sbt assembly)"; exit 1; }
[[ -x "$BIN" ]] || { echo "Missing $BIN — run: (cd rust && cargo build --release)"; exit 1; }

# QUIET=1 so per-row output doesn't pollute timings; both apps honor it.
export QUIET=1

SCALA_CMD=(java -Xmx"$XMX" -jar "$JAR" "$@")
RUST_CMD=("$BIN" "$@")

echo "== wall-clock =="
if command -v hyperfine >/dev/null 2>&1; then
  hyperfine --warmup 2 --runs "$RUNS" \
    -n scala "${SCALA_CMD[*]}" \
    -n rust  "${RUST_CMD[*]}"
else
  echo "(hyperfine not installed: 'cargo install hyperfine'.)"
  echo "Falling back to each app's internal 'Matched rows ... in ms' timer:"
  unset QUIET   # need the summary line visible; it's stderr-free noise otherwise
  echo "-- scala"; "${SCALA_CMD[@]}" | grep -i "matched rows" || true
  echo "-- rust";  "${RUST_CMD[@]}"  | grep -i "matched rows" || true
  export QUIET=1
fi

echo
echo "== peak memory / CPU (single run, /usr/bin/time -v) =="
measure() {
  local name="$1"; shift
  local out; out="$(mktemp)"
  if ! /usr/bin/time -v "$@" >/dev/null 2>"$out"; then
    echo "$name: run failed"; sed 's/^/  /' "$out"; rm -f "$out"; return 1
  fi
  local rss cpu user sys
  rss=$(awk -F': ' '/Maximum resident set size/{print $2}'  "$out")
  cpu=$(awk -F': ' '/Percent of CPU/{print $2}'             "$out")
  user=$(awk -F': ' '/User time/{print $2}'                 "$out")
  sys=$(awk -F': ' '/System time/{print $2}'                "$out")
  printf "%-6s  peakRSS=%9s KB   CPU=%-6s  user=%ss  sys=%ss\n" \
    "$name" "$rss" "$cpu" "$user" "$sys"
  rm -f "$out"
}
measure scala "${SCALA_CMD[@]}"
measure rust  "${RUST_CMD[@]}"

cat <<'NOTE'

Reading the numbers:
  * peakRSS for scala includes the whole JVM (heap + metaspace + runtime), not just
    the workload; capped by JVM_XMX. Rust's is close to the real working set.
  * CPU% > 100 means multiple cores were used. DataFusion parallelizes; parquet4s
    reads single-threaded, so expect rust CPU% > scala CPU%.
  * hyperfine wall-clock includes JVM startup for scala. Compare it against each
    app's internal timer (steady-state query cost) to separate startup from work.
NOTE
