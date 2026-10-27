#!/usr/bin/env bash
# Runs the rendered dump-uploader sidecar in its real image, under the chart's own memory limit,
# in a pod-shaped setup: a pause container owns the PID namespace and the sidecar and a main
# container join it (as with shareProcessNamespace: true). `az` is stubbed so nothing leaves the machine.
#
# Scenarios:
#   real-jvm-oom  - a real JVM (the bench's base image) runs out of heap, writes a heap dump, and exits
#   stalled-write - a dump's writer pauses mid-file for longer than the sidecar's poll interval
#
# Requires: helm, docker. Usage: ./test-dump-uploader.sh
set -euo pipefail

CHART_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK="$(mktemp -d)"
CONTAINERS=()
cleanup() {
  [[ ${#CONTAINERS[@]} -gt 0 ]] && docker rm -f "${CONTAINERS[@]}" >/dev/null 2>&1
  rm -rf "$WORK"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

JVM_IMAGE=eclipse-temurin:25
PAUSE_IMAGE=registry.k8s.io/pause:3.9

helm template test "$CHART_DIR" --set benchType=tpch --set-string providerConfig.nodeConfig=unused \
  --show-only templates/configmap.yaml --show-only templates/benchmark-tpch.yaml > "$WORK/rendered.yaml"

# upload-dumps.sh, de-indented out of the dump-upload-scripts ConfigMap
awk '/^  upload-dumps.sh: \|/ {on=1; next} on && /^[^ ]/ {on=0} on {sub(/^    /, ""); print}' \
  "$WORK/rendered.yaml" > "$WORK/upload-dumps.sh"
[[ -s "$WORK/upload-dumps.sh" ]] || fail "couldn't extract upload-dumps.sh from the chart"

IMAGE=$(grep -A1 'name: dump-uploader' "$WORK/rendered.yaml" | awk '/image:/ {gsub(/"/, "", $2); print $2; exit}')
MEM_LIMIT=$(helm show values "$CHART_DIR" | awk '/^dumpUpload:/ {on=1} on && /limits:/ {l=1} on && l && /memory:/ {gsub(/"/, "", $2); print $2; exit}')
[[ -n "$IMAGE" && -n "$MEM_LIMIT" ]] || fail "couldn't read sidecar image / memory limit from the chart"

# Kubernetes 'Mi' -> docker 'm'
DOCKER_MEM="${MEM_LIMIT%Mi}m"

mkdir -p "$WORK/stub" "$WORK/src"
cat > "$WORK/stub/az" <<'EOF'
#!/bin/bash
# Logs each blob upload with the file's size at upload time, and whether a JVM was still alive.
# With AZ_FAIL_FIRST set, the first upload fails, as a network blip or expired token would.
if [[ -n "${AZ_FAIL_FIRST:-}" && ! -f /out/az-failed-once ]]; then
  touch /out/az-failed-once
  echo "ERROR: simulated upload failure" >&2
  exit 1
fi
name="" file=""
while [[ $# -gt 0 ]]; do
  case "$1" in --name) name="$2" ;; --file) file="$2" ;; esac
  shift
done
alive=dead
for c in /proc/[0-9]*/comm; do [[ "$(cat "$c" 2>/dev/null)" == java ]] && alive=alive; done
echo "$(basename "$name") $(stat -c %s "$file") $alive" >> /out/uploads.log
EOF
chmod +x "$WORK/stub/az"

cat > "$WORK/src/Oom.java" <<'EOF'
import java.util.*;
public class Oom {
  public static void main(String[] args) {
    List<long[]> hog = new ArrayList<>();
    while (true) hog.add(new long[1 << 20]);
  }
}
EOF

cat > "$WORK/src/Idle.java" <<'EOF'
public class Idle {
  public static void main(String[] args) throws InterruptedException {
    Thread.sleep(Long.parseLong(args[0]) * 1000);
  }
}
EOF

docker pull -q "$PAUSE_IMAGE" >/dev/null
docker pull -q "$JVM_IMAGE" >/dev/null

# run_scenario NAME MAIN_IMAGE MAIN_SCRIPT [SIDECAR_ENV]
# Starts the pod's pause container, then the sidecar, then (3s later, as containers start
# concurrently) the main container. Waits for the sidecar to exit and leaves results in $WORK/NAME.
run_scenario() {
  local name=$1 main_image=$2 main_script=$3 sidecar_env=${4:-UNUSED=}
  local out="$WORK/$name" id="dump-uploader-test-$$-$name"
  mkdir -p "$out/dumps"
  CONTAINERS+=("$id-pause" "$id-sidecar" "$id-main")

  docker run -d --name "$id-pause" --ipc shareable "$PAUSE_IMAGE" >/dev/null

  docker run -d --name "$id-sidecar" --pid "container:$id-pause" --platform linux/amd64 \
    -m "$DOCKER_MEM" --memory-swap "$DOCKER_MEM" \
    -v "$WORK/upload-dumps.sh:/scripts/upload-dumps.sh:ro" \
    -v "$WORK/stub:/stub:ro" -v "$out:/out" -v "$out/dumps:/dumps" \
    -e AZURE_STORAGE_ACCOUNT=acct -e AZURE_STORAGE_CONTAINER=ctr \
    -e XTDB_NODE_ID=test-node -e BENCH_TYPE=tpch -e "$sidecar_env" \
    "$IMAGE" bash -c '
      export PATH=/stub:$PATH
      bash /scripts/upload-dumps.sh > /out/sidecar.log 2>&1
      echo $? > /out/exit-code
      grep "^oom_kill " /sys/fs/cgroup/memory.events | cut -d" " -f2 > /out/oom-kills
    ' >/dev/null

  sleep 3
  docker run -d --name "$id-main" --pid "container:$id-pause" \
    -v "$out/dumps:/dumps" -v "$WORK/src:/src:ro" \
    "$main_image" bash -c "$main_script" >/dev/null

  docker wait "$id-main" >/dev/null
  local main_exited=$(date +%s)
  docker wait "$id-sidecar" >/dev/null
  echo $(( $(date +%s) - main_exited )) > "$out/exit-lag"

  echo "=== $name"
  echo "--- sidecar log"; cat "$out/sidecar.log"
  echo "--- uploads (name, size at upload, jvm)"; cat "$out/uploads.log" 2>/dev/null || true
  echo "--- dumps on disk"; ls -l "$out/dumps"
}

# assert_dumps_fully_uploaded NAME: every dump on disk was uploaded, and its last upload carried its final size
assert_dumps_fully_uploaded() {
  local name=$1 out="$WORK/$1" f base final last found=0
  [[ "$(cat "$out/oom-kills")" == 0 ]] || fail "$name: sidecar was OOM-killed under its ${MEM_LIMIT} limit"
  [[ "$(cat "$out/exit-code")" == 0 ]] || fail "$name: sidecar exited $(cat "$out/exit-code")"
  grep -q "peak memory" "$out/sidecar.log" || fail "$name: sidecar didn't log its peak memory"
  # The Job can't complete until the sidecar exits, so it mustn't linger after the JVM has gone
  [[ "$(cat "$out/exit-lag")" -le 30 ]] || fail "$name: sidecar took $(cat "$out/exit-lag")s to exit after the main container"
  for f in "$out"/dumps/*.hprof; do
    [[ -f "$f" ]] || continue
    found=1
    base=$(basename "$f")
    final=$(stat -c %s "$f" 2>/dev/null || stat -f %z "$f")
    last=$(awk -v n="$base" '$1 == n {s = $2} END {print s}' "$out/uploads.log" 2>/dev/null || true)
    [[ -n "$last" ]] || fail "$name: $base was never uploaded"
    [[ "$last" == "$final" ]] || fail "$name: $base's last upload was $last bytes, but the dump is $final bytes"
  done
  [[ $found == 1 ]] || fail "$name: no heap dump was written, so nothing was tested"
}

# A real JVM: same base image as the bench, same dump flags as the chart's jdkOptions
run_scenario real-jvm-oom "$JVM_IMAGE" \
  'java -Xmx256m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/dumps -XX:+ExitOnOutOfMemoryError /src/Oom.java'
assert_dumps_fully_uploaded real-jvm-oom
grep -q '^java_pid.*\.hprof ' "$WORK/real-jvm-oom/uploads.log" || fail "real-jvm-oom: the JVM's own java_pid*.hprof wasn't uploaded"

# A writer that pauses mid-dump for longer than the poll interval, while the JVM stays up
run_scenario stalled-write "$JVM_IMAGE" '
  ( sleep 4;  head -c 1000000 /dev/zero >  /dumps/stalled.hprof
    sleep 12; head -c 1000000 /dev/zero >> /dumps/stalled.hprof ) &
  exec java /src/Idle.java 30'
assert_dumps_fully_uploaded stalled-write
grep -q '^stalled.hprof 2000000 alive$' "$WORK/stalled-write/uploads.log" \
  || fail "stalled-write: the completed dump wasn't uploaded while the JVM was still running"

# The JVM dumps and exits, and the first upload attempt fails: the dump must still get there
run_scenario flaky-upload "$JVM_IMAGE" \
  'java -Xmx64m -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/dumps -XX:+ExitOnOutOfMemoryError /src/Oom.java' \
  AZ_FAIL_FIRST=1
[[ -f "$WORK/flaky-upload/az-failed-once" ]] || fail "flaky-upload: the simulated failure never fired, so nothing was tested"
assert_dumps_fully_uploaded flaky-upload

# An unpinned tag can grow under the memory limit between runs without anything in the repo changing
[[ "$IMAGE" != *:latest ]] || fail "sidecar image is unpinned ($IMAGE)"

echo "PASS"
