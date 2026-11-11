#!/bin/sh
# Runs on a board (testing/board-runs/I6.md), from /run/orion-i6 holding orion, orion-drill and
# scripts/memory-report.sh: Orion with a full set of programs around it, then
# what the comm core and node logic hold. Everything is under /run/orion-i6 and is removed.
set -u
D=/run/orion-i6
S=$D/orion.sock
cleanup() {
  for p in $PIDS; do kill -9 "$p" 2>/dev/null; done
  sleep 0.3
  rm -rf "$D" /var/lib/orion-i6
}
PIDS=""
trap cleanup EXIT
mkdir -p /var/lib/orion-i6
cat > $D/orion.conf <<CONF
node.name = board
socket = $S
state.dir = /var/lib/orion-i6/comm
ssh.listen = 0.0.0.0:2299
peer.listen = 0.0.0.0:9299
ws.listen = 0.0.0.0:9298
CONF
start() { "$@" > "$D/log.$#.$$.$RANDOM" 2>&1 & PIDS="$PIDS $!"; LAST=$!; }
start $D/orion comm $D/orion.conf; COMM=$LAST
ORION_STATE_DIR=/var/lib/orion-i6/node start $D/orion node $D/orion.conf; NODE=$LAST
for i in $(seq 1 50); do $D/orion-drill --address $S node show >/dev/null 2>&1 && break; sleep 0.1; done
$D/orion-drill --address $S node show | head -2
start $D/orion-drill executor $S >/dev/null
start $D/orion-drill provider $S >/dev/null
start $D/orion-drill publisher $S >/dev/null
start $D/orion-drill subscriber $S >/dev/null
start $D/orion-drill subscriber $S >/dev/null
start $D/orion-drill camera $S >/dev/null
start $D/orion-drill viewer $S >/dev/null
start $D/orion-drill viewer $S >/dev/null
sleep 1
for w in 1 2 3 4 5; do
  $D/orion-drill --address $S workload put work$w --kind process --spec "exec sleep 600" >/dev/null 2>&1 || echo "put work$w failed"
done
for t in 1 2 3 4 5 6 7 8; do
  start $D/orion-drill --address $S topic publish sensor.$t value:float=$t --hold 30000
done
sleep 1
$D/orion-drill --address $S action run drill.echo --args hello | head -2
$D/orion-drill --address $S resource list | head -3
for p in $COMM $NODE; do echo 1 > /proc/$p/clear_refs; done
echo "--- steady for 10 s (reference bits cleared at its start)"
sleep 10
echo "--- topics: $($D/orion-drill --address $S topic read drill.count 2>&1 | head -1)"
echo "--- memory (KiB, Pss), comm core and node logic"
$D/memory-report.sh $COMM $NODE
echo "--- threads: comm $(ls /proc/$COMM/task | wc -l), node $(ls /proc/$NODE/task | wc -l)"
echo "--- the other programs"
for p in $PIDS; do [ "$p" = "$COMM" ] || [ "$p" = "$NODE" ] || $D/memory-report.sh $p | head -1; done
echo "--- binary: $(ls -l $D/orion | awk '{print $5}') bytes"
grep -h -i 'warn\|error' $D/log.* | sort | uniq -c | head -10
