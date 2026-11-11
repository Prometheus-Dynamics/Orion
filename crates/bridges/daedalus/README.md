# Topics as Daedalus nodes

A plugin library for `daedalusd`. Put the built library in the daemon's plugin directory and a
graph can read and write Orion topics and take frames from buffer topics.

| Node | Inputs | Outputs |
| --- | --- | --- |
| `orion.topic.read` | `topic: string` | `value: f64`, the newest sample |
| `orion.topic.write` | `topic: string`, `value: f64` | `published: u64`, how many so far |
| `orion.frame.read` | `topic: string` | `frame: bytes`, the newest frame, in place |

- **Every input is connected.** Daedalus gives a plugin's step a cell only for a port with an
  edge, and does not pass a graph's constants to plugin steps. So the topic arrives on an edge,
  and it is also what makes a reader run.
- **A field** is named after a colon: `imu:1` is field 1 of `imu`; `imu` is field 0. A float,
  an integer and a flag all read as `f64`.
- **A write** declares the topic the first time, with one float field `value`.
- **A frame is not copied.** Its buffer is handed to Daedalus where it is, in the publisher's
  shared memory, and held out of the publisher's hands until Daedalus drops the last value of
  it. The owner of the frame's type reads the bytes where they are and makes its own type of
  them.
- **The comm core** is found at `ORION_ADDRESS`, or the board's own socket. The plugin connects
  on the first step that needs it, and again after the connection is lost.

## Build

```bash
cargo build --release
cp target/release/liborion_daedalus.so /var/lib/daedalus/plugins/
```

The plugin and the daemon must come from the same Daedalus commit: the daemon checks the
plugin's step layout when it loads it. `Cargo.toml` pins that commit, and `check.sh` installs
the daemon from it for the tests.
