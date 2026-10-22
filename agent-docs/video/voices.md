# ElevenLabs voice production

Production note, 2026-10-22. The [script](script.md) is the source of spoken words and delivery cues. Its 36 spoken turns cover the narrator and six cartoon characters. The annotations are prepared for audition. No voice takes have been generated.

## Model and speaker assignment

Select `eleven_v4` explicitly for Text to Dialogue. Its square-bracket audio cues also work with `eleven_v3`. See the official [audio-tag help](https://elevenlabs.io/docs/help-center/product/core-capabilities/text-to-speech/how-do-audio-tags-work-with-eleven-v3-and-v4).

Each turn needs its own `text` and selected `voice_id`. Speaker names in the Markdown are production labels. See the official [dialogue guide](https://elevenlabs.io/docs/overview/capabilities/text-to-dialogue).

Keep a stable voice assignment for Narrator, Ishan, Alec, John T, Kyle, Graham, and Ryan. Cast delivery directions describe proposed performances, not the real people's personalities.

## Delivery conventions

Audio tags are natural-language directions rather than a fixed enum. Place them before the words they affect. A later cue changes the delivery within the turn. Voice selection and audition determine the final result. See the official [tag guide](https://elevenlabs.io/blog/elevenlabs-audio-tags-list).

| Cue family | Use in this film |
| --- | --- |
| Cinematic, measured delivery | The opening crawl and final montage. |
| Clear, explanatory delivery | Technical statements and proof shots. |
| Warm, assured delivery | The transition from a working demonstration to its implication. |
| Firm, understated delivery | Short factual emphasis and the final line. |
| Curiosity and thoughtfulness | Questions from the cast. |
| Sarcasm and theatrical alarm | The single compiler-paper joke. |
| Surprise and excitement | The one-process exchange and successful response. |

The script combines documented example tags with custom auditory directions. Examples include `[curious]`, `[sarcastic]`, and `[short pause]`. Phrases such as `[clear, explanatory delivery]` are our proposed directions. Treat every cue as an audition instruction, not a guaranteed switch.

## Pauses and inflection

Use punctuation and bracketed pause cues for relative phrasing. Eleven v4 and Eleven v3 do not support SSML `<break>` tags. See the official [prompting guide](https://elevenlabs.io/docs/overview/capabilities/text-to-speech/best-practices).

Keep exact silence, music cuts, and scene holds in the edit. The opening still has a 55-second budget. Remeasure it after generating the speech. Listen for the transition from cinematic narration to dry dialogue and back.

The crawl text and captions contain only spoken words. Remove the delivery cues. Keep the script's wording intact so that screen text and narration agree.

## Generation procedure

1. Choose the seven voices and rehearse technical pronunciations.
2. Read each spoken paragraph from the script into its turn's `text` value.
3. Retain the bracketed audio cues in that value.
4. Exclude speaker labels, enclosing quotation marks, headings, and stage directions.
5. Assign the selected `voice_id` to each turn.
6. Generate narration by scene and character exchanges as short groups.
7. Use the timestamped dialogue response to prepare timing and mouth cues.
8. Inspect the recorded delivery and alignment before assembling the scene.
9. Retain accepted takes and their model, voice, and text settings for later edits.

The timestamped endpoint currently defaults to `eleven_v3`, so send the chosen model explicitly. Its documented limits allow ten unique voices per request and recommend no more than 2,000 combined text characters. These seven voices fit the voice limit. See the [timestamped API reference](https://elevenlabs.io/docs/api-reference/text-to-dialogue/convert-with-timestamps).

Music and environmental effects remain separate tracks. The script's bracketed cues direct speech performance. Its italic paragraphs direct picture, sound design, and editing.
