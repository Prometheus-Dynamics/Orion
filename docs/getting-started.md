# Getting started

Orion runs as two services from one file, `orion`: the comm core, which is every way into the
board, and node logic, which keeps the records and says what runs. Programs connect to the comm
core's socket. This page runs both on a computer, as an ordinary user, and talks to them.

## Build

```bash
cargo build --release -p orion-bin
```

`target/release/orion` is the whole of it: `orion comm`, `orion node`, and the command line.

## Run

A settings file for a board in a directory of its own (every key is in
[settings.md](settings.md)):

```bash
mkdir -p /tmp/orion && cat > /tmp/orion/orion.conf <<'CONF'
node.name = bench
socket = /tmp/orion/orion.sock
state.dir = /tmp/orion/comm
# Node logic must be let in: here it runs as the same user as the comm core.
socket.node_uids = 1000
CONF
```

Use your own user id (`id -u`) for `socket.node_uids`. Then the two services, each in a
terminal of its own:

```bash
target/release/orion comm /tmp/orion/orion.conf
```

```bash
ORION_STATE_DIR=/tmp/orion/node target/release/orion node /tmp/orion/orion.conf
```

Node logic gets a state directory apart from the comm core's, as on a board. Either can be
stopped and started again while the other runs.

## The command line

Anything that is not `comm` or `node` is the command line. `--address` names the board, here
its socket:

```bash
target/release/orion --address /tmp/orion/orion.sock node show
```

```bash
target/release/orion --address /tmp/orion/orion.sock topic publish greeting text:text=hello --hold 30000
```

```bash
target/release/orion --address /tmp/orion/orion.sock topic read greeting
```

The verbs are in [client.md](client.md), "The command line". Across a network the address is
`orion+tcp://host:port`, with `peer.listen` set on the board.

## The first connection to a board with security on

A board with `security = on` answers a network session only if it is signed, and does for an
operator only what its trust store grants. A tool's key is a file of its own (`--key`, made the
first time it is named). To be let in:

1. **Ask.** From the desk, with this operator's key:

   ```bash
   orion --address orion+tcp://board:9200 --key ~/.config/orion/operator.key operator ask --as desk
   ```

   It prints `operator    pending`, this operator's key (`its key`) and the board's
   (`node key`, with its fingerprint). Compare the fingerprint with the one the board shows
   (`orion node show` on the board) before trusting anything it says.
2. **Be approved,** one of two ways:
   - someone on the board, as root, enrolls that key:
     `orion operator enroll <its key> --as desk`;
   - or the cluster has an enrollment key (`enrollment.key`), and the operator proves it knows it:
     the same `operator ask` with `--enrollment-key-file <file holding it>`. The key itself is
     never sent.

   It prints `operator    enrolled` and what the operator may do (`operators.read`,
   `operators.actions`, or the grants an administrator gave).
3. **Pin the board.** From then on, name the board with its key, so a different board at that
   address is refused, both keys shown: `orion+tcp://board:9200#<node key>`.

Programs do the same through `Orion::enroll` ([client.md](client.md)). With security off none of
this is needed or recorded.

## A program

[`crates/client/orion/examples/first_program.rs`](../crates/client/orion/examples/first_program.rs)
publishes a topic, reads it back as another program would, and asks the board who it is:

```bash
ORION_SOCKET=/tmp/orion/orion.sock cargo run -p orion --example first_program
```

```text
tank.level: 40 l
tank.level: 41.5 l
tank.level: 43 l
the board is bench
```

The `orion` crate is the whole client: topics, frames in shared buffers, records, actions,
files and workloads, each behind a feature ([client.md](client.md)). From C, the same through
`liborion_c` and `crates/client/capi/include/orion.h`;
[`crates/client/capi/tests/c/pubsub.c`](../crates/client/capi/tests/c/pubsub.c) is a whole
program.

## Something to run

Node logic says what runs; an executor runs it. The sample executor runs each workload as a
shell command:

```bash
cargo run -p orion-sample-executor -- --socket /tmp/orion/orion.sock
```

```bash
target/release/orion --address /tmp/orion/orion.sock workload put hello --kind process --spec "while true; do date; sleep 5; done"
```

`workload remove hello` stops it. If the executor is killed, its workloads are lost and are
started again when it is back; if Orion is, they keep running and Orion takes them back
([operating.md](operating.md)).

## On a board

A board runs the two services under systemd, from the files in
[`packaging/`](../packaging/README.md), and is reached the same way, over SSH, the network or a
serial line. [operating.md](operating.md) is what to know about running it.
