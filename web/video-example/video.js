// Video in a page on plain HTTP, two ways. Orion carries bytes; what the bytes are is known
// here and to the publisher, and to nothing in between.
//
// - JPEG: every sample is one picture, whole. Any of them can be lost.
// - H.264: every sample is one fragment of fragmented MP4. A sample others depend on (a key
//   frame) starts with the stream's header, so a page can begin at any one of them, and the
//   board sends nothing that depends on a sample this page never got.
//
// Neither needs a secure page: no WebCodecs.

/**
 * Shows a topic of JPEG pictures in an `<img>`. Only the newest picture is ever waiting to be
 * shown: a page that cannot keep up shows fewer pictures, not older ones.
 */
export async function showJpeg(orion, topic, img) {
  const state = { samples: 0, shown: 0, last: -1n, missed: 0n, close: () => {} };
  let waiting = null;
  let busy = false;
  const show = () => {
    if (busy || waiting === null) return;
    busy = true;
    const url = URL.createObjectURL(new Blob([waiting], { type: "image/jpeg" }));
    waiting = null;
    const done = () => {
      URL.revokeObjectURL(url);
      busy = false;
      show();
    };
    img.onload = () => {
      state.shown += 1;
      done();
    };
    img.onerror = done;
    img.src = url;
  };
  const subscription = await orion.subscribe(topic, (sample) => {
    state.samples += 1;
    if (state.last >= 0n && sample.time > state.last + 1n) state.missed += sample.time - state.last - 1n;
    state.last = sample.time;
    waiting = sample.payload;
    show();
  });
  state.close = () => subscription.close();
  return state;
}

/**
 * Plays a topic of fragmented MP4 in a `<video>` through Media Source. `mime` is what the
 * publisher's encoder makes, for example `video/mp4; codecs="avc1.42E01E"`.
 */
export async function playH264(orion, topic, video, mime) {
  if (!("MediaSource" in window) || !MediaSource.isTypeSupported(mime)) {
    throw new Error("this browser cannot play " + mime);
  }
  const state = { samples: 0, appended: 0, last: -1n, missed: 0n, close: () => {} };
  const source = new MediaSource();
  video.src = URL.createObjectURL(source);
  await new Promise((opened) => source.addEventListener("sourceopen", opened, { once: true }));
  const buffer = source.addSourceBuffer(mime);
  // Fragments play one after another in the order they arrive, whatever times are written
  // in them: a lost stretch leaves no hole to wait at.
  buffer.mode = "sequence";

  const queue = [];
  let started = false;
  const pump = () => {
    if (buffer.updating || source.readyState !== "open") return;
    // Keep ten seconds behind the play head, and stay near the newest picture.
    if (video.buffered.length > 0) {
      const start = video.buffered.start(0);
      const end = video.buffered.end(video.buffered.length - 1);
      if (video.currentTime - start > 20) {
        buffer.remove(start, video.currentTime - 10);
        return;
      }
      if (end - video.currentTime > 2) video.currentTime = end - 0.2;
    }
    const next = queue.shift();
    if (next === undefined) return;
    try {
      buffer.appendBuffer(next);
      state.appended += 1;
    } catch (error) {
      // Full, or the element gave up: begin again at the next key frame.
      started = false;
      queue.length = 0;
    }
  };
  buffer.addEventListener("updateend", pump);

  const subscription = await orion.subscribe(topic, (sample) => {
    state.samples += 1;
    if (state.last >= 0n && sample.time > state.last + 1n) {
      state.missed += sample.time - state.last - 1n;
    }
    state.last = sample.time;
    // Nothing can be shown before a key frame.
    if (!started && !sample.othersDepend) return;
    started = true;
    queue.push(sample.payload);
    pump();
  });
  video.muted = true;
  video.play().catch(() => {});
  state.close = () => {
    subscription.close();
    if (source.readyState === "open") source.endOfStream();
  };
  return state;
}
