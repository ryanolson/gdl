# Opening music

## Source — 2026-10-23

The source is the official [StarWars.com opening-crawl clip](https://www.starwars.com/video/star-wars-episode-iv-a-new-hope-opening-crawl), titled “Star Wars: Episode IV A New Hope - Opening Crawl.” The page publishes its Kaltura entry, `0_gni4sd37`, and media flavor, `1_m4dhyj3w`.

The public [player media URL](https://cdnapi.kaltura.com/p/1068292/sp/62801200/playManifest/entryId/0_gni4sd37/flavorIds/1_m4dhyj3w/format/url/protocol/https) supplies an 87.52-second clip. Its audio is stereo AAC at 44.1 kHz and 128 kb/s. The request required no login, cookies, or supplied credential.

Only the first 36 seconds were decoded for the edit. The retained excerpt uses source time 7.390–31.390 seconds. It contains exactly 24 seconds of audio. The first source sample above magnitude 100 in 16-bit PCM occurs at 7.402333 seconds. That attack occurs 12.333 milliseconds into the retained excerpt.

The user requested this excerpt for a private, noncommercial film with no distribution. The source page states “TM & © Lucasfilm Ltd. All Rights Reserved.” These notes record provenance and do not grant reuse rights.

## Local files

The files remain in the ignored `tools/video/work/media/` directory. Git ignores the local music files.

| File | Contents | Duration | Format |
|---|---|---:|---|
| `star-wars-opening-24s.wav` | Source excerpt, without the film envelope | 24.000 s | 48 kHz, stereo, 16-bit PCM |
| `opening-music.wav` | Music bed aligned to film time zero | 80.000 s | 48 kHz, stereo, 16-bit PCM |

The excerpt SHA-256 is `1b558619e4e438d06d519b9b5e75521527515bec951303c45cf0592dfa792315`. The bed SHA-256 is `75b5bf58a62f375dd54707bb87688f33998b96e8191ab5f9bb1d351b3c8716a9`.

## Mix

The bed holds source gain 1.0 from zero through one second. It fades linearly to 0.12 during seconds one through two. It holds that gain until the final five milliseconds before Kyle interrupts. A five-millisecond fade ends exactly at 24 seconds. The remaining 56 seconds contain digital silence. No voice is mixed into this bed.

PCM measurements show gain 1.000000 from 0–0.99 seconds and 0.120000 from 2–23.99 seconds. The first-second peak is 0.609131, about −4.31 dBFS. The first-second RMS is 0.107583, about −19.37 dBFS. Every sample from 24 seconds onward is zero. The output contains exactly 3,840,000 stereo frames.

## Reproduce

Decode the first 36 seconds from the public player media URL:

```bash
timeout 60 tools/video/node_modules/ffmpeg-static/ffmpeg \
  -nostdin -hide_banner -loglevel error \
  -i 'https://cdnapi.kaltura.com/p/1068292/sp/62801200/playManifest/entryId/0_gni4sd37/flavorIds/1_m4dhyj3w/format/url/protocol/https' \
  -t 36 -vn -c:a pcm_s16le -ar 48000 -ac 2 \
  tools/video/work/media/star-wars-official-source.wav
```

Trim the 24-second excerpt:

```bash
timeout 60 tools/video/node_modules/ffmpeg-static/ffmpeg \
  -nostdin -hide_banner -loglevel error \
  -ss 7.39 -i tools/video/work/media/star-wars-official-source.wav \
  -t 24 -c:a pcm_s16le -ar 48000 -ac 2 \
  tools/video/work/media/star-wars-opening-24s.wav
```

Build the bed with the local mixer:

```bash
timeout 60 node tools/video/music.mjs \
  --music tools/video/work/media/star-wars-opening-24s.wav \
  --out tools/video/work/media/opening-music.wav \
  --duration 80
```

The first audition combines the source excerpt with the aligned `opening-voice.wav` through `music.mjs`. Its mixed track is `tools/video/work/opening-audio.wav`. The browser manifest at `work/media/opening.json` loads the music-only bed and nine separate voice files. The movie uses the mixed track through `render.mjs --audio`.

The exported audition is `tools/video/work/opening-audition.mp4`: 80 seconds, 1920×1080, 24 fps, H.264 video and AAC stereo audio. During seconds 2–22, the voice stem measures −26.4 dB mean volume and the music bed measures −39.1 dB. These FFmpeg measurements establish a 12.7 dB mean separation. Listening and final voice selection remain part of the audition. The cue at 70 seconds and the rest of the score remain pending.
