# Orion

Orchestration and interconnect for devices: the one way in and out of a board, the authority on
what runs there, and the bus values travel on, from a microcontroller up to a PC.

- **One file on a board**, `orion`, run as two services: the comm core, every way in (a socket,
  the network, browsers, SSH, serial and CAN links), and node logic, which keeps the records and
  says what runs.
- **Topics in shared memory.** A publisher writes a ring and its subscribers read it where it
  lies; frames are handed over once, never copied. The same call reaches a topic on another
  board.
- **Workloads, actions, files** through the same connection, each request through one check.
- **Never off for good.** Each part can die and come back without the others noticing; a
  broken board still lets you in.

Start with [docs/getting-started.md](docs/getting-started.md); the rest is listed in
[docs/README.md](docs/README.md).

- [`AGENTS.md`](AGENTS.md): rules for working here.
- [`TODO.md`](TODO.md): where the work stands.
