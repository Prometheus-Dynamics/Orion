#!/usr/bin/env python3
"""WPILib ntcore side of interop/run.sh: an ntcore client against the NT4 gateway.

It reads the mapped topic /Tank/level (double), which must keep going up, and writes
/Panel/setpoint (double), which the gateway publishes on the board.
"""

import argparse
import time

import ntcore


def wait_until(predicate, timeout, what):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise SystemExit(f"timed out waiting for {what}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    inst = ntcore.NetworkTableInstance.create()
    inst.startClient4("ntcore-gateway-interop")
    inst.setServer("127.0.0.1", args.port)
    level = inst.getDoubleTopic("/Tank/level").subscribe(-1.0)
    setpoint = inst.getDoubleTopic("/Panel/setpoint").publish()

    wait_until(inst.isConnected, 10, "the connection")
    wait_until(lambda: level.get() > 0.0, 10, "/Tank/level")
    first = level.get()
    wait_until(lambda: level.get() >= first + 2.0, 10, "/Tank/level to go up")
    topic = inst.getTopic("/Tank/level")
    setpoint.set(7.5)
    # Keep the publication alive long enough for the gateway to take it.
    time.sleep(1.5)
    print(f"ntcore client: /Tank/level type={topic.getTypeString()} first={first} later={level.get()}")


if __name__ == "__main__":
    main()
