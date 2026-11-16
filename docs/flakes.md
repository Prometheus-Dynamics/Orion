# Flakes

A test that passed only when it was run again has a bug in it: the test's, or the code's. Each one
is written down here with how many times a rerun was what made a run green. Each is fixed by its
cause, never with a retry. One that is fixed stays on the list with its cause and fix, so the
same shape is recognised next time.

| Test | Reruns to green | Symptom | Cause | Fixed in |
| --- | --- | --- | --- | --- |
| `drills` `workloads.rs` `orion_dies_whole_and_takes_back_what_kept_running` | 1 (a gate of a commit's tree, under parallel load) | the subscriber role: `failed Connection refused`, and no `seen 300` within 10 s | The test killed Orion before the subscriber role had connected. Under load the role was still in its own connect loop (a fixed 20 ms retry that gave up after 10 s) and met only a socket with no listener. The library also had no first connect that keeps trying. | C61 (the obvious connect keeps trying), C62 (the roles use it; the test waits for both roles to be in before the kill) |
| `connect` `alive.rs` `a_node_silent_for_its_idle_time_is_gone_and_silent_for_says_so` | 1 (in 40 runs under full load) | `after 598ms`, below the 600 ms idle time | The test timed the silence from its question, but silence counts from the last frame heard (the welcome), which came earlier, and longer before under load. | C62 (it measures `silent_for` when the session is found gone) |
| `connect` `alive.rs`, a board-socket test (which one was not recorded) | 1 (the C59 gate) | `ConnectionReset` on the board socket | Two tests in one process bound the same socket path, `orion-alive-silent-<pid>`. One test's client reached the other's fake node, which accepts once. Which test lost depended on timing. Under load after C61, the same collision hung a test: the second client waited in a backlog no one accepted from. | C62 (each fake node gets a path of its own, counted, whatever its test's name) |
| `shell` `shell.rs` `with_a_login_helper_the_helper_starts_the_shell` | open: 1 (the D16 gate); 0 in 20 quiet runs, 30 under full load, and 40 of the whole binary under full load | `not opened on a terminal`: the third shell, on a terminal, was answered with something other than `Opened` | Not found yet. Ruled out: output after `ShellExit` (the comm core drains a shell's output before it), and the helper going idle (a held shell keeps it running). The test now says what it heard instead. | |
