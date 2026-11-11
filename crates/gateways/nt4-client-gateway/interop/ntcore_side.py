#!/usr/bin/env python3
"""WPILib ntcore side of interop/run.sh: an ntcore server that the gateway joins.

It holds /Robot/enabled (boolean), which the gateway publishes on the board, and reads
/Tank/level (double), which the board publishes here and which must keep going up.
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
    inst.startServer(listen_address="127.0.0.1", port4=args.port)
    enabled = inst.getBooleanTopic("/Robot/enabled").publish()
    enabled.set(False)
    level = inst.getDoubleTopic("/Tank/level").subscribe(-1.0)

    wait_until(lambda: level.get() > 0.0, 15, "/Tank/level")
    first = level.get()
    wait_until(lambda: level.get() >= first + 2.0, 10, "/Tank/level to go up")
    topic = inst.getTopic("/Tank/level")
    enabled.set(True)
    time.sleep(1.5)
    print(f"ntcore server: /Tank/level type={topic.getTypeString()} first={first} later={level.get()}")


if __name__ == "__main__":
    main()
