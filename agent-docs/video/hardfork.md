# Hard Fork post-credits sources

Research date: 2026-10-22. The requested tribute uses the real two-word catchphrase with original bobblehead animation. The source identities and YouTube caption times below are verified. The podcast audio candidates have not been checked by listening and must not enter the production mix yet.

Current production status: the optional 505–512s visual ending, two portraits, archive cue IDs `hardfork-casey` and `hardfork-kevin`, and a silent seven-second animatic are ready. The audio-to-cue mapping remains unverified. Credit the hosts and episode as listed below before accepting the final takes.

## Official episode

The episode is **Meta Shifts the Blame + Do Data Center Bans Work? + The Final HatGPT**, published August 28, 2026, episode 210 of Hard Fork. The [official podcast feed](https://feeds.simplecast.com/l2i9YnTd) identifies The New York Times as its author and gives a duration of 1:02:56. Its GUID is `acceb119-5645-4f38-a562-9c3b7198b254`.

The [NYT episode page](https://www.nytimes.com/2026/08/28/podcasts/hardfork-meta-settlement.html) supplies the named-speaker transcript. Indexed NYT transcript text identifies Kevin Roose's standalone line after the Vision Pro price joke and Casey Newton's line after the founders' insomnia exchange. Normal browser access to the NYT page was blocked by browser security policy during this research; no workaround was attempted after that block.

The [official YouTube upload](https://www.youtube.com/watch?v=1YpqU1LerjM) has a different title: **What Meta's $17 Billion Settlement Means for TikTok and YouTube**. Its author is Hard Fork, channel `UCZcR2SVWaGWNlMqPxvQS3vw`, published August 28, 2026. Its metadata gives 3,552 seconds, or 59:12. The description marks HatGPT at 40:10.

## Caption targets

| Speaker | Official YouTube caption time | Context used to align the speaker | Status |
| --- | --- | --- | --- |
| Kevin Roose | [43:44](https://www.youtube.com/watch?v=1YpqU1LerjM&t=2624s) | Immediately after the Vision Pro price joke. The next turn starts at 43:45. | Standalone “Stop generating.” The caption gives whole-second boundaries; the audio still needs a precise trim. |
| Casey Newton | [50:44](https://www.youtube.com/watch?v=1YpqU1LerjM&t=3044s) | After his reply about also waking up with anxiety. | The phrase shares a caption with his next sentence. Trim to the two requested words after listening. |
| Casey Newton, alternate | [58:45](https://www.youtube.com/watch?v=1YpqU1LerjM&t=3525s) | End of his closing request to Kevin. | The phrase crosses the 58:44–58:45 captions and is part of a longer sentence. |

The browser's documented `exportYouTubeTranscript()` operation exported the official upload's English auto-generated captions. The export is a research artifact, not a checked-in transcript: `/var/folders/gz/c44sd99n1yl7j50x8tlvz_bc0000gn/T/browser-use/exports/youtube-1YpqU1LerjM-a53744e6-7fdc-43b6-9282-fc58cfe3eb48.txt`. It contains 74,499 bytes; SHA-256 `f2fccec0c732d8e4ca88ef8f3c48cd39b3a1a0c573a539d93fd5db6091b829a2`.

## Public audio source and candidate files

The feed exposes an `audio/mpeg` [official MP3 enclosure](https://dts.podtrac.com/redirect.mp3/pdst.fm/e/tracking.swap.fm/track/4jdbjyyrkqk96egqozqb/nyt.simplecastaudio.com/3e43d072-f8a5-430f-bc8e-4c70aafdf3c7/episodes/95108f33-c923-4c83-837e-f48795e7ae23/audio/128/default.mp3?aid=rss_feed&awCollectionId=3e43d072-f8a5-430f-bc8e-4c70aafdf3c7&awEpisodeId=95108f33-c923-4c83-837e-f48795e7ae23&feed=l2i9YnTd). The Simplecast episode ID is `95108f33-c923-4c83-837e-f48795e7ae23`. A normal HEAD request followed the public redirects to `nyt.simplecastaudio.com`, which reported byte-range support and a 60,423,261-byte response. The RSS enclosure's declared length is 60,424,942 bytes. The feed has no chapter or transcript tags for this episode.

Three three-second candidates were extracted with project-local ffmpeg from the public enclosure. These are search windows around supplied timing leads. Their contents and speaker assignments remain unverified. Each MP3 has 49,066 bytes and reports 3.03 seconds because of MP3 encoding delay and padding.

The extraction used the enclosure URL above as `HARDFORK_SOURCE` and changed the start time and output name for each candidate:

```bash
timeout 35 tools/video/node_modules/ffmpeg-static/ffmpeg -hide_banner -loglevel warning -ss 00:47:56 -rw_timeout 10000000 -i "$HARDFORK_SOURCE" -t 3 -map 0:a:0 -c:a libmp3lame -b:a 128k tools/video/work/media/hardfork-candidate-4756.mp3
```

| Work file | Requested podcast range | SHA-256 |
| --- | --- | --- |
| `tools/video/work/media/hardfork-candidate-4756.mp3` | 47:56–47:59 | `6f1ef212b6e0570825fa9d6271faf573a883dbee22f145b7adf5e54a45fecc19` |
| `tools/video/work/media/hardfork-candidate-5101.mp3` | 51:01–51:04 | `0ca6e6514d40940f8bc3e1a2dafebf4e4f867a862514f42cfa1a9326e2d5002f` |
| `tools/video/work/media/hardfork-candidate-5545.mp3` | 55:45–55:48 | `b64baeafed7ed7c43c6427eed18888ca01e83fe66bed4f23f824dd25a6d21ee7` |

## Timing and editing procedure

The podcast and YouTube editions have different runtimes. The YouTube opening starts with the Meta discussion, while the podcast transcript starts with a restaurant anecdote. The public audio also travels through an advertising delivery chain. These facts make a single assumed offset unsafe. Third-party transcript times are search leads, not final cut points.

1. Use the official YouTube targets to identify the two performances and their context.
2. Listen to the bounded podcast candidates to establish their contents and speaker mapping. The browser's supported media-download operation was attempted on the official YouTube video, but it cannot download that player's blob source. No video source file was saved. No downloader or speech-recognition package is installed for this task.
3. Keep only the two-word performances. Record exact source in/out times, output duration, and output hash after the trim. Do not use a context window as the final asset.
4. Label the finished scene as a Hard Fork tribute. Credit Kevin Roose, Casey Newton, Hard Fork, The New York Times, the episode date, and the source link. Keep this real excerpt separate from the fictional cast dialogue and ElevenLabs takes.

The ending's existing archive cues carry source recordings, with no ElevenLabs model or TTS text. No cloned host voices or accepted final audio cuts have been created. This research changed only this ledger and the candidate files; the root agent owns the script and stage integration.
