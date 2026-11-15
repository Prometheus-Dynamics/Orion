#!/bin/sh
# testing/board-runs/C48.md, board side, from /run/orion-m2 (orion, orion-drill,
# memory-report.sh). The board's own Orion is not touched. Everything but the logs
# (/run/orion-m2-logs) is removed at the end.
set -u
D=/run/orion-m2
S=$D/orion.sock
PIDS=""
BEFORE=$(ps -o pid,args | awk '$2=="sleep" && $3=="600" {print $1}' | tr '\n' ' ')
cleanup() {
  for p in $PIDS; do kill -9 "$p" 2>/dev/null; done
  sleep 0.3
  # Only the workloads this run started: any `sleep 600` that was there before is left.
  for p in $(ps -o pid,args | awk '$2=="sleep" && $3=="600" {print $1}'); do
    case " $BEFORE " in *" $p "*) ;; *) kill -9 "$p" 2>/dev/null ;; esac
  done
  # The logs are kept, outside the directory that is removed.
  mkdir -p /run/orion-m2-logs && cp $D/log.* /run/orion-m2-logs/ 2>/dev/null
  rm -rf "$D"
  echo "--- cleanup: $(ls -d $D 2>&1 | head -1)"
}
trap cleanup EXIT
mkdir -p $D/state
cat > $D/orion.conf <<CONF
node.name = m2
socket = $S
state.dir = $D/state
keys.dir = $D/state/keys
ssh.listen = 127.0.0.1:2399
peer.listen = 127.0.0.1:9399
ws.listen = 127.0.0.1:9398
CONF
start() { "$@" > "$D/log.$$.$RANDOM" 2>&1 & PIDS="$PIDS $!"; LAST=$!; }
up() { for i in $(seq 1 100); do $D/orion-drill --address $S node show >/dev/null 2>&1 && return 0; sleep 0.05; done; return 1; }
uptime_ms() { awk '{printf "%d", $1*1000}' /proc/uptime; }
start $D/orion comm $D/orion.conf; COMM=$LAST
ORION_STATE_DIR=$D/state/node start $D/orion node $D/orion.conf; NODE=$LAST
up || echo "NOT UP"
echo "--- node show"; $D/orion-drill --address $S node show
start $D/orion-drill executor $S
start $D/orion-drill provider $S
start $D/orion-drill publisher $S
start $D/orion-drill subscriber $S
start $D/orion-drill subscriber $S
start $D/orion-drill camera $S
start $D/orion-drill viewer $S
start $D/orion-drill viewer $S
sleep 1
for w in 1 2 3 4 5; do
  $D/orion-drill --address $S workload put work$w --kind process --spec "exec sleep 600" >/dev/null 2>&1 || echo "put work$w failed"
done
for t in 1 2 3 4 5 6 7 8; do
  start $D/orion-drill --address $S topic publish sensor.$t value:float=$t --hold 30000
done
sleep 1
echo "--- action"; $D/orion-drill --address $S action run drill.echo --args hello 2>&1 | head -4
echo "--- resources"; $D/orion-drill --address $S resource list 2>&1 | head -4
echo "--- workloads running: $(ps -o args | awk '$1=="sleep" && $2=="600"' | wc -l)"
for p in $COMM $NODE; do echo 1 > /proc/$p/clear_refs; done
t0c=$(awk '{print $14+$15}' /proc/$COMM/stat); t0n=$(awk '{print $14+$15}' /proc/$NODE/stat)
echo "--- steady for 10 s"
sleep 10
echo "--- cpu ticks in 10 s: comm $(( $(awk '{print $14+$15}' /proc/$COMM/stat) - t0c )), node $(( $(awk '{print $14+$15}' /proc/$NODE/stat) - t0n )) (CLK_TCK $(getconf CLK_TCK 2>/dev/null || echo 100))"
echo "--- memory (KiB, Pss), comm core and node logic"
$D/memory-report.sh $COMM $NODE
echo "--- threads: comm $(ls /proc/$COMM/task | wc -l), node $(ls /proc/$NODE/task | wc -l)"
echo "--- binary: $(ls -l $D/orion | awk '{print $5}') bytes"

echo "=== kill node logic"
kill -9 $NODE; sleep 0.3
echo "resources without node logic: $($D/orion-drill --address $S resource list 2>&1 | head -1)"
a=$(uptime_ms)
ORION_STATE_DIR=$D/state/node start $D/orion node $D/orion.conf; NODE=$LAST
for i in $(seq 1 200); do $D/orion-drill --address $S resource list >/dev/null 2>&1 && break; sleep 0.01; done
echo "node logic answered after $(( $(uptime_ms) - a )) ms (from its start, tries 10 ms apart)"
echo "workloads running after: $(ps -o args | awk '$1=="sleep" && $2=="600"' | wc -l)"

echo "=== kill comm core"
kill -9 $COMM; sleep 1
a=$(uptime_ms)
start $D/orion comm $D/orion.conf; COMM=$LAST
up && echo "comm core answered after $(( $(uptime_ms) - a )) ms"
sleep 3
echo "reconnected lines: $(grep -l -i reconnected $D/log.* 2>/dev/null | wc -l) programs"
echo "topic read after: $($D/orion-drill --address $S topic read drill.count 2>&1 | head -1)"
echo "workloads running after: $(ps -o args | awk '$1=="sleep" && $2=="600"' | wc -l)"

echo "--- warnings and errors in all logs"
grep -h -i 'warn\|error\|panic' $D/log.* | sed 's/^[0-9TZ:.-]* *//' | sort | uniq -c | sort -rn | head -15
