# ElevenLabs voice production

The optional [Hard Fork tribute](script.md#optional-post-credits-tag--a-nod-to-hard-fork) uses original archive clips for Casey Newton and Kevin Roose. Exclude `source: "archive"` cues from ElevenLabs. Those cues have no TTS text and `model_id: null`. Their [source ledger](hardfork.md) records clip status.

Production note, 2026-10-22. The [script](script.md) is the source of spoken words and delivery cues. Its 39 spoken turns cover the narrator and six cartoon characters across 8:25. The annotations are prepared for audition. No voice takes have been generated.

## Account setup

The first audition covers the 80-second opening: nine turns, seven voices, and 936 tagged input characters. The complete script contains 6,106 tagged input characters. These counts exclude the two archive cues. Retakes need additional credits.

- Use an ElevenLabs account with API access and available credits.
- Create the key under Developers > API Keys. The [official setup guide](https://elevenlabs.io/docs/help-center/technical/how-do-i-authorize-myself-using-an-api-key) describes these controls.
- Store a restricted API key in 1Password. Supply the item name or `op://` reference, not the key value.
- Enable `text_to_speech`, `voices_read`, and `models_read`. The [official API schema](https://api.elevenlabs.io/openapi.json) defines these permissions. `user_read` is optional for a quota lookup.
- Supply Ryan's custom `voice_id` when it is ready. A stock voice can cover his rehearsal turns.
- Select the other stock voices during audition. Shared-library imports need the optional `add_voice_from_voice_library` permission.

Inject the credential through the workstation's secret tooling. Keep credentials out of scripts, generated cue sheets, audio metadata, and logs. Music and sound-effects permissions are separate from speech and are not required for these voice auditions.

## Model and speaker assignment

Select `eleven_v4` explicitly for Text to Dialogue. Its square-bracket audio cues also work with `eleven_v3`. See the official [audio-tag help](https://elevenlabs.io/docs/help-center/product/core-capabilities/text-to-speech/how-do-audio-tags-work-with-eleven-v3-and-v4).

Each turn needs its own `text` and selected `voice_id`. Speaker names in the Markdown are production labels. See the official [dialogue guide](https://elevenlabs.io/docs/overview/capabilities/text-to-dialogue).

Keep a stable voice assignment for Narrator, Ishan, Alec, John T, Kyle, Graham, and Ryan. Cast delivery directions describe proposed performances, not the real people's personalities.

Use stock voices for the cast and narrator. Ryan can supply a custom ElevenLabs voice for himself. A stock Ryan voice remains available for rehearsal until that voice exists.

| Speaker | Performance |
| --- | --- |
| Narrator | Measured cinematic opening, clear technical explanation, restrained closing. |
| Kyle | Friendly impatience, crisp interruptions, and emphatic questions. |
| Ishan | Casual confidence with light Gen Z slang. |
| Ryan | One puzzled translation request in the opening. Technical and assured delivery afterward. |
| John T | Calm, precise explanation. |
| Graham | Dry delivery before the compiler-paper reveal. |
| Alec | One brief theatrical reaction. |

## Delivery conventions

Audio tags are natural-language directions rather than a fixed enum. Place them before the words they affect. A later cue changes the delivery within the turn. Voice selection and audition determine the final result. See the official [tag guide](https://elevenlabs.io/blog/elevenlabs-audio-tags-list).

| Cue family | Use in this film |
| --- | --- |
| Cinematic, measured delivery | The opening crawl and final montage. |
| Clear, explanatory delivery | Technical statements and proof shots. |
| Warm, assured delivery | The transition from a working demonstration to its implication. |
| Firm, understated delivery | Short factual emphasis and the final line. |
| Curiosity and thoughtfulness | Questions from the cast. |
| Jumping in | Kyle interrupts the crawl, then asks about re-exports and graph versus placement changes. |
| Sarcasm and theatrical alarm | The single compiler-paper joke. |
| Surprise and excitement | The one-process exchange and successful response. |

The script combines documented example tags with custom auditory directions. Examples include `[curious]`, `[sarcastic]`, and `[short pause]`. Phrases such as `[clear, explanatory delivery]` and `[jumping in]` are our proposed directions. Treat every cue as an audition instruction, not a guaranteed switch.

## Pauses and inflection

Use punctuation and bracketed pause cues for relative phrasing. Eleven v4 and Eleven v3 do not support SSML `<break>` tags. See the official [prompting guide](https://elevenlabs.io/docs/overview/capabilities/text-to-speech/best-practices).

Keep exact silence, music cuts, and scene holds in the edit. The opening now ends at 80s. Remeasure it after generating the speech. Kyle makes the first audible break from the crawl. Start that turn crisply, with the music cut on “Wait.” Keep his later questions curious, impatient, and friendly. Listen for the transition from cinematic narration to dry dialogue and back.

The interruption runs from 24–80s. Each first voice receives a first-name label and a face hold of at least one second. Preserve the quiet gaps between these turns:

| Time | Turn |
| --- | --- |
| 24–28s | Kyle interrupts. Hold through 30s. |
| 30–34s | Ishan uses one slang line. Hold through 36s. |
| 36–39s | Ryan asks for a translation. Hold through 42s. |
| 42–46s | John T explains typed links. Hold through 49s. |
| 49–53s | Graham introduces the compiler error. Hold through 56s. |
| 56–60s | Alec reacts. Hold through 63s. |
| 63–67s | Ryan returns to assured technical delivery. Hold through 70s. |
| 70–80s | The narrator resolves the introduction. |

Ryan asks about the slang exactly once. His later lines retain technical confidence. The brief exchange supplies humor without delaying the working demonstration.

Give “configuration contract” and “metric definitions” separate emphasis in the ACME and placement scenes. Leave the detailed TOML and metric labels to the picture. Reserve enough quiet time for the audience to compare the same node before and after its placement change.

The crawl text and captions contain only spoken words. Remove the delivery cues. Keep the script's wording intact so that screen text and narration agree.

## Generation procedure

1. Choose seven stock voices for rehearsal and rehearse technical pronunciations.
2. Read each spoken paragraph from the script into its turn's `text` value.
3. Retain the bracketed audio cues in that value.
4. Exclude speaker labels, enclosing quotation marks, headings, and stage directions.
5. Assign the selected `voice_id` to each turn.
6. Generate narration by scene and character exchanges as short groups.
7. Use the timestamped dialogue response to prepare timing and mouth cues.
8. Inspect the recorded delivery and alignment before assembling the scene.
9. Retain accepted takes and their model, voice, and text settings for later edits.

If Ryan supplies a custom voice, replace his stock assignment before the final takes. Keep his accepted voice consistent across both opening turns and later technical lines.

The timestamped endpoint currently defaults to `eleven_v3`, so send the chosen model explicitly. Its documented limits allow ten unique voices per request and recommend no more than 2,000 combined text characters. These seven voices fit the voice limit. See the [timestamped API reference](https://elevenlabs.io/docs/api-reference/text-to-dialogue/convert-with-timestamps).

Music and environmental effects remain separate tracks. The script's bracketed cues direct speech performance. Its italic paragraphs direct picture, sound design, and editing.
