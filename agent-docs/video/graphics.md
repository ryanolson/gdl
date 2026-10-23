# Talking heads and opening graphics

Creative brief, 2026-10-22. The owner selected cartoon portraits based on reference photos. Cast: Ishan, Alec, John T, Kyle, Graham, and Ryan. The [script](script.md) defines the dialogue and timing. The [history ledger](history.md) separates supplied quotations from dramatization.

Implementation status, 2026-10-22: the HTML stage and all six neutral masters exist. Current head motion uses deterministic bob and rotation. Mouth states, blink assets, and phoneme alignment remain pending. The silent 80-second opening exports at 1920×1080 and 24fps. Actual composer recordings include the API rehearsal and a 33.52-second tour of the two film packages, configuration controls, graph, segments, and Deploy. Accepted voices and final ACME proof clips remain production inputs.

Ending addendum, 2026-10-22: the owner requested a Hard Fork tribute with Casey Newton and Kevin Roose bobbleheads. Their transparent neutral heads are in `assets/casey/` and `assets/kevin/`. Each directory records the exact prompt and official photo sources. The optional HTML tag uses staggered spring entrances and a hard cut to black. Open `?ending=hardfork&t=505` to rehearse it. Original archive takes remain separate inputs. The [source ledger](hardfork.md) records their status.

Audio addendum, 2026-10-23: the first opening audition combines the existing animation, nine generated voice takes, and the requested main-title music cue. See the [voice brief](voices.md) and [music sources](music.md). The portraits still use neutral heads with deterministic motion. Mouth and blink variants remain pending.

## Portrait inputs

All six labeled reference photos are supplied. Preserve glasses, facial hair, hair shape, and other identifying features present in each reference.

The six neutral masters below supply the talking heads. Expression variants remain pending. John T wears the owner's requested red Canada cap with a white maple leaf.

Color revision, 2026-10-22: the owner requested color for Ryan after seeing the stage. His active portrait uses warm fair peach skin and brown hair. His original reference remains grayscale. The added colors are artistic choices.

| Screen label | Reference photo | Opening beat |
| --- | --- | --- |
| Ishan | Supplied; [neutral master](assets/ishan/neutral.png) | Says “No cap. This graph is going to cook.” |
| Alec | Supplied; [neutral master](assets/alec/neutral.png) | Reacts to the compiler-paper avalanche. |
| John T | Supplied; [neutral master](assets/john/neutral.png) | Connects matching typed ports, wearing his Canada cap. |
| Kyle | Supplied; [neutral master](assets/kyle/neutral.png) | Interrupts the crawl first and draws a process boundary. |
| Graham | Supplied; [neutral master](assets/graham/neutral.png) | Opens the terminal, then helps complete the graph. |
| Ryan | Supplied; [neutral master](assets/ryan/neutral.png) | Asks about the slang once, then reveals the composable graph. |

These are proposed stage actions, not descriptions of the people's personalities or historical performances.

## Shared portrait treatment

Create recognizable cartoon likenesses with bold flat shapes, restrained detail, and a handmade paper-cut finish. Use simple outlines and consistent shading. Keep the faces expressive and readable against the control room. Use the same rendering style and relative head size for all six.

Frame each head and neck on a square transparent canvas. Leave space around the hair. The head sits above a separate paper body. Keep body and arm motion simple so the face carries the performance.

Use each person's neutral master as the reference for every expression variant. Inspect each edit against that master. Keep the camera, crop, neck anchor, face proportions, hair, glasses, facial hair, and requested cap fixed. Keep Ryan's warm skin tones and brown hair fixed. Inspect the cast together for consistent scale and treatment during scene assembly.

| State | Purpose |
| --- | --- |
| `neutral` | Closed-mouth listening pose and held frames. |
| `talk` | Open-mouth speech pose. |
| `exclaim` | Rounded-mouth emphasis for the interruption. |
| `blink` | Closed eyes with the neutral mouth. |

The planned set contains six master portraits and eighteen edited variants. Every image needs genuine transparency. The browser stage can swap aligned states and moves the head as one piece. The current stage uses neutral portraits only.

Stage production assets in `agent-docs/video/assets/`, with subdirectories `ishan`, `alec`, `john`, `kyle`, `graham`, and `ryan`. Each directory contains `neutral.png` and `prompt.md`. Add `talk.png`, `exclaim.png`, and `blink.png` as aligned edits. Only neutral states exist so far. Keep production assets in the repository as well as the generator cache.

## Portrait generation specification

Use the built-in image tool with the labeled photo as a likeness reference. Generate one asset per person. Then edit that master for the other states. Do not regenerate the identity independently for each expression.

Apply this shared specification to each named reference:

```text
Use case: stylized-concept
Asset type: transparent cartoon talking head for a cut-paper animated film
Subject: [screen label], based on the labeled reference photo
Input image role: likeness reference
Primary request: create a recognizable cartoon portrait of this exact person
Style: flat paper-cut cartoon, simple bold shapes, consistent restrained outlines, minimal shading
Framing: front-facing head and neck, square canvas, whole hair silhouette visible, centered neck anchor, room around the head
Expression: neutral, closed mouth, eyes open
Background: genuinely transparent
Preserve: the reference person's face proportions, hair, glasses, facial hair, and distinguishing visible features
Color palette: preserve the active master's palette, including Ryan's owner-directed warm skin tones and brown hair
Requested accessories: preserve John T's red Canada cap and white maple leaf in every state
Constraints: no text, no props, no invented accessories, no extra people
```

For an expression edit, change only the mouth or eyelids. Preserve the entire remaining portrait and canvas alignment. Inspect the result against the master before using it.

## Opening picture and sound

| Time | Picture | Sound |
| --- | --- | --- |
| 00:00–00:02 | Star field and the opening heading. | A quiet cinematic rise. |
| 00:02–00:24 | Gold perspective crawl with original text from the script. Hold the last line for two seconds. | The narrator reads those exact words from 00:02–00:22. |
| 00:24–00:28 | Kyle breaks into the stars, then draws a boundary in the control room. His first-name spotlight starts immediately. | Kyle: “Wait. Who decides where it runs?” Music stops on “Wait.” Quiet beat follows. |
| 00:30–00:34 | Ishan receives the spotlight and ISHAN label from 00:29–00:35. | Ishan: “No cap. This graph is going to cook.” Hold the reaction before Ryan. |
| 00:36–00:39 | Ryan receives the spotlight and RYAN label from 00:35–00:40. | Ryan: “Are we compiling something, or cooking it?” This is his only translation question. |
| 00:42–00:46 | John T receives the spotlight and JOHN T label from 00:41–00:47. He connects typed ports. | John T: “The links already check the types.” A small paper sound follows. |
| 00:49–00:53 | Graham receives the spotlight and GRAHAM label from 00:48–00:54. He opens the terminal. | Graham: “The compiler would like a word.” The paper starts after his line. |
| 00:56–01:00 | Alec receives the spotlight and ALEC label from 00:55–01:01. The paper reaches his desk and coffee. | Alec: “Oh, the humanity.” Hold the reaction before Ryan returns. |
| 01:03–01:07 | Ryan rolls the paper aside. Graham adds the matching adapter. The cast remains a tableau through 01:10. | Ryan: “Let's make the whole thing composable.” |
| 01:10–01:20 | Paper graph becomes the actual composer. CUT HERE title. | Music returns. The narrator establishes that pipelines worked and introduces the GDL workflow. |

These are editorial picture cues. Adjust the performances to preserve the 80-second introduction before the catalog section. The full film ends at 505s, or 8:25.

The stage derives the first-introduction spotlights from `scene.introductions`. Each first cast line after Kyle receives a one-second lead and one-second trail. Kyle interrupts immediately in his dedicated 24–28s window. The spotlight enlarges the featured head and dims the other characters. Returning dialogue and narration do not repeat an introduction.

Kyle returns twice with purposeful questions: can a re-export preserve the contract, and are we changing the graph or its process boundaries? These exchanges lead into the catalog and NIM-shaped demonstrations. They remain fictional dialogue.

Set the crawl as live browser text. Keep the visible words identical to the narrator's words. Use CSS perspective and an explicit scene clock. The screen text must remain readable at the chosen capture size. Do not generate a flattened image of the crawl.

The compiler paper is a theatrical prop. Suggest the scale of a difficult trait error with nested type fragments. Label the exchange as dramatization. Do not pass the prop off as actual output from the current GDL build.

Give each character one compact name label on first appearance. Use the supplied screen labels until the owner provides different display names.

## Browser and capture behavior

The browser stage source lives in `tools/video/dist/`. Its crawl and paper room share a deterministic scene clock. Separate layers hold the background, bodies, portrait states, props, names, and captions. `tools/video/README.md` records the local run and rendering procedure.

Support play, pause, replay, and seeking to an exact cue. Keep controls outside the capture area. Scene selection must reset every portrait, prop, and audio cue. Do not time the mouth motion from unrelated page timers.

After accepted voices and expression assets exist, use the recorded timings for mouth swaps. Speech can alternate neutral and talk states at a restrained rate. Reserve exclaim for emphasis. Keep blinking occasional and deterministic. The [script's audio tags](script.md) guide the voice performance. Use the resulting recordings to time expressions. Remove the tags from visible text and captions.

Keep dialogue performances separate from the narrator, music, and effects. Use stock voices for the cast and narrator. Ryan can supply a custom ElevenLabs voice for himself. Reference photos supply appearance, not voice recordings.

Capture the actual `gdl-web` catalog, Graph, Segments, and Deploy views for the technical scenes. Preserve the actual UI and outputs in those takes. Synthetic screenshots do not satisfy these shots. Use the stage for animation, captions, transitions, and timed playback of captured footage.

The movie route combines exact stage frames, recorded composer and terminal clips, and a separately mixed audio track into MP4. The opening has recorded voices and music for audition. Remaining narration and the finished full film remain pending. Missing demo footage must stay visibly incomplete in an animatic.

## Asset checks

- The six identities match their labeled photos and share the same cartoon treatment.
- Every head has a complete hair silhouette and clean transparent edges.
- Each person's states share the same crop, canvas, and neck anchor.
- Mouth and blink changes preserve identity and identifying features.
- Faces remain readable at their actual screen size.
- Name labels and captions do not overlap the graph or portrait features.
- Each first cast introduction has a readable name and spotlight hold. Ryan's translation question appears only in the opening.
- The crawl remains readable and synchronized with the narration.
- The paper gag ends before the live demonstration. Proof shots keep tool output visible.
