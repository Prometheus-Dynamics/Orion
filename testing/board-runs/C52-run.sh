#!/bin/sh
# C52b on a test board: the login helper as root with no bounding set (a root login is all of root),
# and the comm core as uid 902 with only SYS_TIME and NET_BIND_SERVICE, each in a transient
# systemd unit (c52-*, --collect, RuntimeMaxSec so it ends by itself if this script dies), the
# way the shipped units run them. Everything in /run/c52; nothing on disk; no board service,
# module or slot touched. Preflight first: any missing command or flag stops with "NOT RUN".
# A rehearsal on a PC sets C52_REHEARSAL=1: user units as the user running it, logging in as
# that user, its own directory. On the board nothing is set and none of it applies.
D=${C52_DIR:-/run/c52}
MAX=600
if [ "${C52_REHEARSAL:-0}" = 1 ]; then
  COMM_UID=$(id -u); ACCOUNT=$(id -un); SR="systemd-run --user"; SC="systemctl --user"
  SANDBOX_USER=$ACCOUNT
  AS_COMM=""; HELPER_CAPS=""; COMM_CAPS=""; COMM_AMBIENT=""
else
  # systemd starts a unit only as an account the board knows, and the test board's image has
  # no orion-comm (902). These two exist and run nothing: the comm core as daemon, the
  # sandbox's child as bin, two accounts as on a board with the shipped users.
  COMM_USER=daemon; SANDBOX_USER=bin
  COMM_UID=$(awk -F: -v u=$COMM_USER '$1==u {print $3}' /etc/passwd)
  ACCOUNT=root; SR="systemd-run"; SC="systemctl"
  AS_COMM="--uid=$COMM_USER --gid=$COMM_USER"
  HELPER_CAPS=""
  COMM_CAPS="CapabilityBoundingSet=CAP_SYS_TIME CAP_NET_BIND_SERVICE"
  COMM_AMBIENT="AmbientCapabilities=CAP_SYS_TIME CAP_NET_BIND_SERVICE"
fi

say() { echo "== $*"; }

# --- Preflight -------------------------------------------------------------------------
missing=""
for c in systemd-run systemctl ssh ssh-keygen awk grep sed cat sleep kill date lsmod md5sum ps cmp chown chmod mkdir tr cut netstat; do
  command -v "$c" > /dev/null 2>&1 || missing="$missing $c"
done
if [ -n "$missing" ]; then echo "NOT RUN: missing:$missing"; exit 2; fi
for flag in --unit --collect --uid --gid --property --quiet; do
  $SR --help 2>&1 | grep -q -- "$flag" || { echo "NOT RUN: systemd-run has no $flag"; exit 2; }
done
[ -x $D/orion ] || { echo "NOT RUN: $D/orion is not there"; exit 2; }
if [ "${C52_REHEARSAL:-0}" != 1 ]; then
  for u in $COMM_USER $SANDBOX_USER; do
    grep -q "^$u:" /etc/passwd || { echo "NOT RUN: no account $u"; exit 2; }
    ps -o user | grep -qx "$u" && { echo "NOT RUN: $u runs something"; exit 2; }
  done
  [ -n "$COMM_UID" ] || { echo "NOT RUN: no uid for $COMM_USER"; exit 2; }
fi
[ -n "$($SC list-units 'c52-*' --all --no-legend)" ] && { echo "NOT RUN: c52-* units already there"; exit 2; }
netstat -ltn 2> /dev/null | grep -q ':2222 ' && { echo "NOT RUN: port 2222 is taken"; exit 2; }
say "preflight: every command and flag there; no c52-* unit; 2222 free"
[ "${C52_PREFLIGHT_ONLY:-0}" = 1 ] && { say "preflight only: comm core as ${COMM_USER:-me} ($COMM_UID), sandbox as $SANDBOX_USER"; exit 0; }

# --- Before ----------------------------------------------------------------------------
say "before: board clock $(date -u +%FT%TZ) (the host's clock is a week ahead: the old image's time handling)"
$SC list-units --type=service --state=running --no-legend --plain > $D/services.before
$SC --failed --no-legend --plain > $D/failed.before
lsmod > $D/modules.before
say "before: $(wc -l < $D/services.before) services running, $(wc -l < $D/failed.before) failed, modules $(md5sum < $D/modules.before | cut -c1-12)"

HOLD=""
cleanup() {
  [ -n "$HOLD" ] && kill "$HOLD" 2> /dev/null
  $SC stop c52-comm.service c52-login.service 2> /dev/null
  sleep 1
  $SC list-units --type=service --state=running --no-legend --plain > $D/services.after
  $SC --failed --no-legend --plain > $D/failed.after
  lsmod > $D/modules.after
  for what in services failed modules; do
    if cmp -s $D/$what.before $D/$what.after; then r=same; else r=CHANGED; fi
    say "after: $what $r"
    [ "$r" = CHANGED ] && diff $D/$what.before $D/$what.after 2> /dev/null | head -10
  done
  left=$($SC list-units 'c52-*' --all --no-legend)
  [ -n "$left" ] && echo "LEFT UNITS: $left" || say "after: no c52-* unit"
  left=$(ps -o pid,args 2> /dev/null | grep -E "^ *[0-9]+ $D/orion ")
  [ -n "$left" ] && echo "LEFT RUNNING: $left"
  say "after: board clock $(date -u +%FT%TZ)"
  rm -rf $D
  say "cleaned: $D removed"
}
trap cleanup EXIT INT TERM

# --- Setup -----------------------------------------------------------------------------
# The comm core's own directories, as systemd gives the shipped unit (StateDirectory,
# RuntimeDirectory): it makes its socket and keys in them.
mkdir -p $D/state $D/comm
chown $COMM_UID:$COMM_UID $D/state $D/comm
cat > $D/orion.conf << EOF
node.name = c52
socket = $D/comm/orion.sock
state.dir = $D/state
ssh.listen = 127.0.0.1:2222
ssh.helper = $D/orion
login.helper = $D/login.sock
login.comm_user = $COMM_UID
login.idle_ms = 120000
ssh.sandbox_user = $SANDBOX_USER
EOF
chmod 644 $D/orion.conf
chmod 755 $D
ssh-keygen -q -t ed25519 -N "" -f $D/key > /dev/null

# --- The helper, then the comm core ----------------------------------------------------
$SR --quiet --unit=c52-login --collect -p RuntimeMaxSec=$MAX \
  ${HELPER_CAPS:+-p "$HELPER_CAPS"} \
  -p StandardOutput=file:$D/helper.log -p StandardError=file:$D/helper.log \
  $D/orion login-helper $D/orion.conf
i=0; while [ ! -S $D/login.sock ] && [ $i -lt 50 ]; do sleep 0.1; i=$((i+1)); done
[ -S $D/login.sock ] || { echo "FAIL: the helper did not listen"; cat $D/helper.log; exit 1; }
$SR --quiet --unit=c52-comm --collect -p RuntimeMaxSec=$MAX $AS_COMM \
  ${COMM_AMBIENT:+-p "$COMM_AMBIENT"} ${COMM_CAPS:+-p "$COMM_CAPS"} -p NoNewPrivileges=yes \
  -p StandardOutput=file:$D/comm.log -p StandardError=file:$D/comm.log \
  $D/orion comm $D/orion.conf
i=0; while ! netstat -ltn 2> /dev/null | grep -q ':2222 ' && [ $i -lt 100 ]; do sleep 0.1; i=$((i+1)); done
netstat -ltn 2> /dev/null | grep -q ':2222 ' || { echo "FAIL: the comm core does not listen"; cat $D/comm.log; exit 1; }

pid_of() { $SC show -p MainPID --value "$1"; }
caps() { awk '/^(Uid|Gid|Groups|CapInh|CapPrm|CapEff|CapBnd|CapAmb|NoNewPrivs|VmRSS)/ {printf "  %s %s\n", $1, $2}' /proc/$1/status; }
SSH="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=$D/known -o BatchMode=yes -i $D/key -p 2222"
HELPER=$(pid_of c52-login.service); COMM=$(pid_of c52-comm.service)
say "helper $HELPER, idle"; caps $HELPER

# --- A session held open ---------------------------------------------------------------
$SSH $ACCOUNT@127.0.0.1 'sleep 15' > /dev/null 2>&1 &
HOLD=$!
sleep 4
say "helper $HELPER, with a session open"; caps $HELPER
say "comm core $COMM"; caps $COMM
for pid in $(ps -o pid,args | grep -E "^ *[0-9]+ $D/orion " | awk '{print $1}'); do
  [ "$pid" = "$HELPER" ] || [ "$pid" = "$COMM" ] && continue
  echo "== other: $(tr '\0' ' ' < /proc/$pid/cmdline | cut -c1-60) ($pid)"; caps $pid
done
for pid in $(ps -o pid,args | grep "sleep 15" | grep -v grep | grep -v ssh | awk '{print $1}'); do
  say "the session's shell ($pid)"; caps $pid
done

# --- A command, and what it is ---------------------------------------------------------
CAPS='grep -E "^(Uid|CapPrm|CapEff|CapBnd|NoNewPrivs)" /proc/self/status'
say "a root login runs:"
$SSH $ACCOUNT@127.0.0.1 "$CAPS" 2>&1 | sed 's/^/  /'
# A login as an account that is not root: the first with a uid of 1000 or more and a shell.
OTHER=$(awk -F: '$3 >= 1000 && $3 < 65534 && $7 !~ /(nologin|false)$/ {print $1; exit}' /etc/passwd)
if [ -n "$OTHER" ] && [ "${C52_REHEARSAL:-0}" != 1 ]; then
  say "a login as $OTHER runs:"
  $SSH $OTHER@127.0.0.1 "$CAPS" 2>&1 | sed 's/^/  /'
else
  say "a login that is not root: not tried here (${OTHER:-no account with uid >= 1000 and a shell})"
fi
wait $HOLD 2> /dev/null; HOLD=""
say "helper log"; sed 's/^/  /' $D/helper.log | tail -40
say "comm log"; sed 's/^/  /' $D/comm.log | tail -60
