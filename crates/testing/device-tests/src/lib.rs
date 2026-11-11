//! Microcontrollers end to end. The tests are in `tests/`: a device built from the device
//! crates runs on one end of a pseudo-terminal, a board with node logic serves the other end
//! as a serial link, and an ordinary client finds the device, reads it and drives it.
