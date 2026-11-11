# Video in a browser

A page that shows two video topics from a board, on plain `http://`:

- **JPEG**: each sample is one picture, shown in an `<img>`.
- **H.264**: each sample is one fragment of fragmented MP4, played in a `<video>` through
  Media Source. A sample that starts a group of pictures is published as one others depend
  on, and begins with the stream's header (`ftyp` and `moov`), so a page can begin at any of
  them.

Orion moves the bytes. Only the publisher and `video.js` know what they are.

Neither way needs a secure page. WebCodecs would, which is why it is not used.

## Run it

Publish the two topics on the board. `media/` holds a short test clip in both forms, and
the publisher that plays it in a loop is an example of `orion-web-tests`:

    cargo run -p orion-web-tests --example publish-video -- /run/orion/orion.sock web/video-example/media 10

Serve `web/` over HTTP from anywhere, and open

    http://<host>/video-example/index.html?board=ws://<board>:<ws port>/orion&jpeg=camera.jpeg&h264=camera.h264&mime=video/mp4;%20codecs=%22avc1.42D00B%22

The board needs `ws.listen` set, and the page's origin allowed in `ws.origins`.

## On a slow link

The board keeps a limited number of bytes waiting for each subscriber (`outbound.max_bytes`).
Past that it drops samples for that subscriber alone, and after a drop it sends nothing that
depended on what was dropped: the next thing the page gets on the H.264 topic is a key
frame. `video.js` plays fragments in the order they arrive, so a dropped stretch is a jump,
not a stall. For JPEG a drop is a picture never shown.

## The media

Made with:

    ffmpeg -f lavfi -i testsrc=size=160x120:rate=10 -frames:v 16 -q:v 10 frame-%02d.jpg
    ffmpeg -f lavfi -i testsrc=size=160x120:rate=10 -frames:v 16 -c:v libopenh264 -g 8 -bf 0 \
        -b:v 120k -pix_fmt yuv420p -movflags frag_every_frame+empty_moov+default_base_moof clip.mp4
