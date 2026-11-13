# TODO

Every planned PR into `next`. `python3 scripts/pr-status.py` shows the live state: a merged PR is one whose `pr/<id>` branch merged. Briefs are in [`docs/prs/`](docs/prs/). The design is [`docs/design.md`](docs/design.md).

## A. Foundation

Types, the wire, the check and the tools everyone builds on. A1 to A5 go one at a time, then the group fans out.

- [x] [A1](docs/prs/A1.md) Clean slate and skeleton (wave 1; needs nothing)
- [x] [A2](docs/prs/A2.md) Boundary checks in the gate (wave 2; needs A1)
- [x] [A3](docs/prs/A3.md) orion-types: names, ids, time (wave 2; needs A1)
- [x] [A4](docs/prs/A4.md) Wire framing and the kind table (wave 3; needs A3)
- [x] [A5](docs/prs/A5.md) Session kinds: hello, roles, refusal (wave 4; needs A4)
- [x] [A6](docs/prs/A6.md) Bodies: records (wave 5; needs A5)
- [x] [A7](docs/prs/A7.md) Bodies: topics (wave 5; needs A5)
- [x] [A8](docs/prs/A8.md) Bodies: actions (wave 5; needs A5)
- [x] [A9](docs/prs/A9.md) Bodies: workloads and executors (wave 5; needs A5)
- [x] [A10](docs/prs/A10.md) Bodies: files (wave 5; needs A5)
- [x] [A11](docs/prs/A11.md) Bodies: peers, trust, time (wave 5; needs A5)
- [x] [A12](docs/prs/A12.md) Recorded bytes and fuzzing (wave 6; needs A6, A7, A8, A9, A10, A11)
- [x] [A13](docs/prs/A13.md) The one check (wave 3; needs A3)
- [x] [A14](docs/prs/A14.md) Failure rule helper (wave 3; needs A3)
- [x] [A15](docs/prs/A15.md) Allocation and cost counters (wave 2; needs A1)
- [x] [A16](docs/prs/A16.md) File-descriptor passing (wave 2; needs A1)
- [x] [A17](docs/prs/A17.md) Config and the security switch (wave 3; needs A3)
- [x] [A18](docs/prs/A18.md) Profile dev's memory (wave 1; needs nothing; board run)

## B. Rings

The shared-memory ring that values and frames travel through. The riskiest code in the rebuild, so it is proven alone first.

- [x] [B1](docs/prs/B1.md) Ring format, writer and reader (wave 3; needs A3, A15)
- [x] [B2](docs/prs/B2.md) Ring concurrency proof (wave 4; needs B1)
- [x] [B3](docs/prs/B3.md) Doorbells (wave 4; needs B1, A16)
- [x] [B4](docs/prs/B4.md) Latest values and expiry (wave 4; needs B1)
- [x] [B5](docs/prs/B5.md) Lookup by frame time (wave 5; needs B1, B3)
- [x] [B6](docs/prs/B6.md) Typed and structured values (wave 6; needs A7)
- [x] [B7](docs/prs/B7.md) Buffer topics: pool and entries (wave 4; needs B1, A16)
- [x] [B8](docs/prs/B8.md) Hold marks and limits (wave 5; needs B7, B3)
- [x] [B9](docs/prs/B9.md) Ring costs on the test board (wave 6; needs B3, B5, B8, A15; board run)

## C. Comm core

The always-up program: connections, the broker, hello, discovery, time and links.

- [x] [C1](docs/prs/C1.md) Comm server: sessions and dispatch (wave 5; needs A5, A13, A14, A17)
- [x] [C2](docs/prs/C2.md) Same-board carrier (wave 6; needs C1, A16)
- [x] [C3](docs/prs/C3.md) Topic broker (wave 6; needs C1, A7, B3, B4)
- [x] [C4](docs/prs/C4.md) Publishing on behalf, and demand (wave 7; needs C3)
- [x] [C5](docs/prs/C5.md) Buffer topics in the broker (wave 7; needs C3, B8)
- [x] [C6](docs/prs/C6.md) Network carrier: orion+tcp (wave 7; needs C1, C7)
- [x] [C7](docs/prs/C7.md) Keys, enrollment, operators (wave 6; needs A11, A13)
- [x] [C8](docs/prs/C8.md) Hello document (wave 7; needs C1, C7)
- [x] [C9](docs/prs/C9.md) Discovery (wave 8; needs C6, C7)
- [x] [C10](docs/prs/C10.md) Time sync (wave 7; needs C1, C3)
- [x] [C11](docs/prs/C11.md) Serial link (wave 6; needs C1)
- [x] [C12](docs/prs/C12.md) CAN link (wave 7; needs C11)
- [x] [C13](docs/prs/C13.md) USB link (wave 7; needs C11; board run)
- [x] [C14](docs/prs/C14.md) Host facts and Orion's own state as topics (wave 7; needs C3)
- [x] [C15](docs/prs/C15.md) Comm assembly (wave 8; needs C2, C3, C4, C5, C6, C8, C10, C14)
- [x] [C16](docs/prs/C16.md) Adopting a publisher's own ring and pool (wave 18; needs J2)
- [x] [C17](docs/prs/C17.md) Withdrawing one topic (wave 18; needs C16)
- [x] [C18](docs/prs/C18.md) The caller in a claim (wave 18)
- [x] [C20](docs/prs/C20.md) Adopted topics, as their first consumer found them (wave 18; needs C16)
- [x] [C21](docs/prs/C21.md) Orion's reader slots in an adopted table (wave 18; needs C16)
- [x] [C22](docs/prs/C22.md) How long a session may go quiet, settable (wave 18)
- [x] [C24](docs/prs/C24.md) SSH on one interface (wave 18)
- [x] [C19](docs/prs/C19.md) What the board is doing, and Orion's protocol in recovery (wave 18)
- [x] [C25](docs/prs/C25.md) State directories made and tried before the start (wave 18)
- [x] [C27](docs/prs/C27.md) A shell on the board over the session (wave 18)
- [x] [C26](docs/prs/C26.md) A USB link outlives the host going away (wave 18)

## D. Client

One client with the same calls on any address, plus the C surface and orionctl.

- [x] [D1](docs/prs/D1.md) Client: connect to any address (wave 7; needs A5, C2)
- [x] [D2](docs/prs/D2.md) Client: topics (wave 8; needs D1, B3, B4, B5, B6, C3)
- [x] [D3](docs/prs/D3.md) Client: buffer topics (wave 9; needs D2, B8, C5)
- [x] [D4](docs/prs/D4.md) Client: records (wave 8; needs D1, A6)
- [x] [D5](docs/prs/D5.md) Client: actions (wave 8; needs D1, A8)
- [x] [D6](docs/prs/D6.md) Client: files (wave 8; needs D1, A10)
- [x] [D7](docs/prs/D7.md) Client: executor role (wave 8; needs D1, A9)
- [x] [D8](docs/prs/D8.md) Client: provider role (wave 9; needs D4, D5)
- [x] [D9](docs/prs/D9.md) The orion facade (wave 10; needs D2, D3, D4, D5, D6, D7, D8)
- [x] [D10](docs/prs/D10.md) C surface (wave 11; needs D9)
- [x] [D11](docs/prs/D11.md) orionctl (wave 11; needs D9)
- [x] [D13](docs/prs/D13.md) The first connection: enrolling an operator (wave 18)
- [x] [D14](docs/prs/D14.md) Which key answered, when it is not the one pinned (wave 18)

## E. Node logic

Records, peer sync, the workload tracker, actions and files.

- [x] [E1](docs/prs/E1.md) Record store (wave 6; needs A6)
- [x] [E2](docs/prs/E2.md) Persistence (wave 7; needs E1)
- [x] [E3](docs/prs/E3.md) Node as the comm core's first client (wave 8; needs E2, D1)
- [x] [E4](docs/prs/E4.md) Workload tracker (wave 7; needs E1, A9)
- [x] [E5](docs/prs/E5.md) Executors: start, stop, adopt (wave 10; needs E4, D7, I3)
- [x] [E6](docs/prs/E6.md) Stopping and force remove (wave 11; needs E5)
- [x] [E7](docs/prs/E7.md) Files before start (wave 11; needs E5, E13)
- [x] [E8](docs/prs/E8.md) Peer sync (wave 8; needs E1, A11, C6)
- [x] [E9](docs/prs/E9.md) Placement and leases (wave 9; needs E8, E4)
- [x] [E10](docs/prs/E10.md) Actions: the open list (wave 9; needs E3, A8, A13)
- [x] [E11](docs/prs/E11.md) Forwarded actions (wave 10; needs E10, E8)
- [x] [E12](docs/prs/E12.md) Detached actions (wave 10; needs E10)
- [x] [E13](docs/prs/E13.md) Files: store and receive (wave 9; needs E3, A10)
- [x] [E14](docs/prs/E14.md) Files: pull by URL (wave 10; needs E13)
- [x] [E15](docs/prs/E15.md) Files: straight to the destination (wave 10; needs E13)
- [x] [E16](docs/prs/E16.md) Resources (wave 10; needs E1, D8)
- [x] [E17](docs/prs/E17.md) Audit log (wave 9; needs E3)
- [x] [E18](docs/prs/E18.md) Node assembly (wave 12; needs E5, E6, E7, E9, E11, E12, E14, E15, E16, E17)
- [x] [E19](docs/prs/E19.md) Peer sync on connections (wave 13; needs E8, E11, E18)
- [x] [E20](docs/prs/E20.md) Placement on connections (wave 14; needs E9, E19)
- [x] [E21](docs/prs/E21.md) Forgiving a stale peer (wave 14; needs E19)

## F. Leaving the board

Other boards, browsers and NT4.

- [x] [F1](docs/prs/F1.md) Topics across boards (wave 8; needs C3, C6)
- [x] [F2](docs/prs/F2.md) Outbound limits and the drop rule (wave 7; needs C3)
- [x] [F3](docs/prs/F3.md) WebSocket carrier (wave 8; needs C1, F2)
- [x] [F4](docs/prs/F4.md) TypeScript client (wave 9; needs F3)
- [x] [F5](docs/prs/F5.md) Browser video example (wave 10; needs F4)
- [x] [F6](docs/prs/F6.md) NT4 protocol crate (wave 2; needs A1)
- [x] [F7](docs/prs/F7.md) NT4 gateway (wave 9; needs F6, C3, F3)
- [x] [F8](docs/prs/F8.md) NT4 gateway: client form (wave 10; needs F7; board run)
- [x] [F9](docs/prs/F9.md) NT4 on the WebSocket listener (wave 10; needs F7, F3)
- [x] [F10a](docs/prs/F10a.md) A signed session on a link (wave 18)

## G. Access and recovery

SSH served by Orion, the recovery shell and the recovery build.

- [x] [G1](docs/prs/G1.md) SSH server (wave 7; needs C1, C7)
- [x] [G2](docs/prs/G2.md) sftp and scp (wave 8; needs G1)
- [x] [G3](docs/prs/G3.md) Port forwarding (wave 8; needs G1)
- [x] [G4](docs/prs/G4.md) Pre-auth sandbox (wave 8; needs G1)
- [x] [G5](docs/prs/G5.md) Recovery shell (wave 8; needs G1)
- [x] [G6](docs/prs/G6.md) Recovery build (wave 9; needs G5, C15, C13; board run)

## H. Microcontrollers

The device side on the same wire, with its size gate.

- [x] [H1](docs/prs/H1.md) Device core (wave 5; needs A5)
- [x] [H2](docs/prs/H2.md) Device topics (wave 6; needs H1, A7)
- [x] [H3](docs/prs/H3.md) Device actions (wave 6; needs H1, A8)
- [x] [H4](docs/prs/H4.md) Device firmware receive (wave 6; needs H1, A10)
- [x] [H5](docs/prs/H5.md) Host side of devices (wave 10; needs C11, C3, D8)
- [x] [H6](docs/prs/H6.md) Firmware send (wave 11; needs H5, H4, E13)
- [x] [H7](docs/prs/H7.md) MCU template (wave 7; needs H2, H3, H4)
- [ ] [H8](docs/prs/H8.md) STM32G4 and the size gate (wave 11; needs H7, H5; board run)

## I. Finish

One binary, packaging, the graph bridge (dropped), failure drills and the hardware proof.

- [x] [I1](docs/prs/I1.md) One binary (wave 13; needs C15, E18, D11)
- [ ] [I2](docs/prs/I2.md) Graph bridge, dropped (wave 10; needs D2, D3)
- [x] [I3](docs/prs/I3.md) Sample executor (wave 9; needs D7)
- [x] [I4](docs/prs/I4.md) Packaging (wave 14; needs I1)
- [x] [I5](docs/prs/I5.md) Failure drills (wave 14; needs I1, I3)
- [x] [I6](docs/prs/I6.md) Memory and size gates (wave 14; needs I1; board run)
- [x] [I7](docs/prs/I7.md) End-to-end proof on the test board (wave 15; needs I1, I2, I4, F1, F5, F7, G6; board run)
- [x] [I8](docs/prs/I8.md) User documentation (wave 14; needs I1)
- [x] [I9](docs/prs/I9.md) Clean up the plan (wave 16; needs I5, I6, I7, I8)

## J. The stack around Orion

Agreed with the libraries around Orion on 2026-11-11.

- [x] [J1](docs/prs/J1.md) Opaque data: sample times and buffer topics (wave 17)
- [x] [J2](docs/prs/J2.md) The shared ring crates (wave 18; needs J1; waits on the device library)
- [x] [J3](docs/prs/J3.md) Name the clock (wave 17; needs J1)
- [x] [J4](docs/prs/J4.md) Board management is the device library's (wave 17)
- [x] [J5](docs/prs/J5.md) The gate fails on a stale lockfile (wave 18)
- [x] [J6](docs/prs/J6.md) Small boards: three targets, BusyBox init, a flasher's recovery (wave 18)
- [x] [J7](docs/prs/J7.md) The shm-* pins at the revision everyone holds (wave 18)
- [x] [J8](docs/prs/J8.md) The shm-* pins at the device library's d6b9361d (wave 18)

## Open

- What wakes a graph. Every topic gives a subscriber something it can sleep on, so any answer works.
- Pinning a graph to a core. A workload's placement has room for a CPU set (A9).
