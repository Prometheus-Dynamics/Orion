#!/usr/bin/env python3
"""WPILib ntcore side of scripts/nt4-interop.sh. Not part of the default CI.

  ntcore-client --port P   connect to an orion-nt4 server (nt4-server) on P: publish a double[]
                           and a string, subscribe to a double and a string the server holds.
  ntcore-server --port P   serve a double, boolean, string[], int and raw topic on P for a while.
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


def ntcore_client(port):
    inst = ntcore.NetworkTableInstance.create()
    inst.startClient4("ntcore-interop")
    inst.setServer("127.0.0.1", port)
    number = inst.getDoubleTopic("/interop/server/number").subscribe(-1.0)
    label = inst.getStringTopic("/interop/server/label").subscribe("")
    array = inst.getDoubleArrayTopic("/interop/client/array").publish()
    text = inst.getStringTopic("/interop/client/text").publish()
    array.set([1.0, 2.5])
    text.set("hello-from-ntcore")

    wait_until(inst.isConnected, 10, "the connection")
    wait_until(lambda: number.get() == 1.5, 10, "/interop/server/number = 1.5")
    wait_until(lambda: label.get() == "from-orion", 10, "/interop/server/label")
    # Keep the publications alive long enough for the server to record them.
    time.sleep(2)
    print(
        f"ntcore client: number={number.get()} label={label.get()!r} "
        f"server_time_offset_us={inst.getServerTimeOffset()}"
    )


def ntcore_server(port, seconds):
    inst = ntcore.NetworkTableInstance.create()
    inst.startServer(listen_address="127.0.0.1", port4=port)
    double = inst.getDoubleTopic("/interop/wpi/double").publish()
    boolean = inst.getBooleanTopic("/interop/wpi/bool").publish()
    names = inst.getStringArrayTopic("/interop/wpi/names").publish()
    count = inst.getIntegerTopic("/interop/wpi/count").publish()
    raw = inst.getRawTopic("/interop/wpi/raw").publish("raw")
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        double.set(2.25)
        boolean.set(True)
        names.set(["a", "b"])
        count.set(7)
        raw.set(bytes([1, 2]))
        time.sleep(0.1)
    print("ntcore server: done")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="role", required=True)
    client = sub.add_parser("ntcore-client")
    client.add_argument("--port", type=int, required=True)
    server = sub.add_parser("ntcore-server")
    server.add_argument("--port", type=int, required=True)
    server.add_argument("--seconds", type=float, default=12.0)
    args = parser.parse_args()
    if args.role == "ntcore-client":
        ntcore_client(args.port)
    else:
        ntcore_server(args.port, args.seconds)


if __name__ == "__main__":
    main()
