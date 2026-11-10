# The client

What a program uses to talk to a comm core. One client, with the same calls wherever the comm
core is.

## Connecting

`orion-connect` gives a `Session`.

| Address | Written | What it is |
| --- | --- | --- |
| Local | `/run/orion/orion.sock` or `unix:<path>` | the socket of the comm core on this board. Descriptors travel here and nowhere else. |
| Network | `orion+tcp://host[:port]`, with `#<key in hex>` to accept only that node | a comm core on the network. Port 9200 unless told. |
| Serial | `serial:<path>[@baud]` | a comm core at the other end of a serial line, 115200 unless told. |

`Session::over_link` takes a link that someone else opened.

With a key in its options a client's network session is signed: the node knows who is calling,
the client checks the node holds its key, and nothing can be changed on the way. Without one it
is plain, which a node with security on refuses.

**The session is blocking and owns no thread and no timer.** `next(wait)` waits for the next
frame and, while it waits, answers the comm core's pings, sends its own, and notices when the
comm core has said nothing for three heartbeats. A program with nothing to wait for calls it
with a zero wait from its own loop; one that waits on other things too gives `as_fd` to `poll`.
With feature `tokio`, `next_async` waits inside a runtime instead.

**There is one way back.** A call that fails with an error for which `is_lost` is true means
the connection broke or the comm core went quiet. `reconnect` connects again to the same
address as the same program, a short fixed time between tries, for as long as the caller
says. A connection to the comm core is a way in, and those are not backed off. An answer of
"no" (a rejected hello, a key that is not welcome) is returned at once: asking again does not
change it.
