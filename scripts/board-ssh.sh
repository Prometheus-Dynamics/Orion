#!/usr/bin/env bash
# SSH to a reference board, accepting only the host key pinned for it here. The addresses are
# the machine's, not the repository's: `setup` writes them into WORK/ssh_config (WORK is
# ORION_WORK, /mnt/nvme-wd/orion/cache if unset), with a known_hosts file of its own, so the
# user's ~/.ssh is never read or changed.
#
#   scripts/board-ssh.sh setup NAME=[JUMP_USER@JUMP_HOST/]USER@HOST...    write the config
#   scripts/board-ssh.sh NAME [COMMAND...]                                ssh, key checked first
#
# The first connection to a board learns its key, and that key is held to the pin below before
# any command runs; a key that does not match is removed and refused.
set -euo pipefail
work="${ORION_WORK:-/mnt/nvme-wd/orion/cache}"
config="$work/ssh_config"
known="$work/known_hosts"

# The host key each board must present (ED25519, SHA-256 fingerprints).
pin() {
  case "$1" in
    board1) echo "SHA256:t9zt9CfP5fxbKh4tJdgPGytLjPHF5+PNQ6vJ/N4Kjss" ;;
    board2) echo "SHA256:RZsQKK5rgRNQAmcJOQ70gL78oYQs//LjiMXZjQL6g6w" ;;
    *) return 1 ;;
  esac
}

if [ "${1:-}" = setup ]; then
  shift
  mkdir -p "$work"
  : > "$config.tmp"
  for entry in "$@"; do
    name="${entry%%=*}" where="${entry#*=}" jump=""
    pin "$name" > /dev/null || { echo "no pinned key for $name" >&2; exit 2; }
    case "$where" in */*) jump="${where%%/*}" where="${where#*/}" ;; esac
    {
      echo "Host $name"
      echo "  HostName ${where#*@}"
      echo "  User ${where%@*}"
      [ -z "$jump" ] || echo "  ProxyJump $jump"
      echo "  HostKeyAlias $name"
      echo "  UserKnownHostsFile $known"
      echo "  StrictHostKeyChecking accept-new"
      echo "  BatchMode yes"
    } >> "$config.tmp"
  done
  # A jump host too keeps its key here, never in ~/.ssh.
  printf 'Host *\n  UserKnownHostsFile %s\n  StrictHostKeyChecking accept-new\n' "$known" >> "$config.tmp"
  mv "$config.tmp" "$config"
  echo "wrote $config"
  exit 0
fi

name="${1:?usage: scripts/board-ssh.sh setup ... | NAME [COMMAND...]}"
shift
want="$(pin "$name")" || { echo "no pinned key for $name" >&2; exit 2; }
[ -f "$config" ] || { echo "no $config: run scripts/board-ssh.sh setup first" >&2; exit 2; }
ssh -F "$config" "$name" true
have="$(ssh-keygen -lF "$name" -f "$known" | awk '/^[^#]/ {print $2; exit}')"
if [ "$have" != "$want" ]; then
  ssh-keygen -R "$name" -f "$known" > /dev/null 2>&1 || true
  echo "refused: $name presented $have, not the pinned $want" >&2
  exit 1
fi
exec ssh -F "$config" "$name" "$@"
