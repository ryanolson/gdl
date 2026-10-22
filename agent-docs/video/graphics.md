# Talking heads and opening graphics

Creative brief, 2026-10-22. The owner selected cartoon portraits based on reference photos. Cast: Ishan, Alec, John T, Kyle, Graham, and Ryan. The [script](script.md) defines the dialogue and timing. The [history ledger](history.md) separates supplied quotations from dramatization.

## Portrait inputs

Each person needs one clear, labeled reference photo. A front or slight three-quarter view works for this composition. Keep the full head visible. Preserve glasses, facial hair, hair shape, and other identifying features present in the reference.

The supplied attachment contains text, not photos. The portrait files remain pending. Do not infer anyone's appearance from a first name or substitute a generic face.

| Screen label | Reference photo | Opening beat |
| --- | --- | --- |
| Ishan | Pending | Opens with “This seems straightforward.” |
| Alec | Pending | Reacts to the compiler-paper avalanche. |
| John T | Pending | Connects matching typed ports. |
| Kyle | Pending | Draws a process boundary. |
| Graham | Pending | Opens the terminal, then helps complete the graph. |
| Ryan | Pending | Reveals the composable graph. |

These are proposed stage actions, not descriptions of the people's personalities or historical performances.

## Shared portrait treatment

Create recognizable cartoon likenesses with bold flat shapes, restrained detail, and a handmade paper-cut finish. Use simple outlines and consistent shading. Keep the faces expressive and readable against the control room. Use the same rendering style and relative head size for all six.

Frame each head and neck on a square transparent canvas. Leave space around the hair. The head sits above a separate paper body. Keep body and arm motion simple so the face carries the performance.

Build each person's master portrait first. Inspect all six together before creating expression variants. Use the master as the reference for every variant. Keep the camera, crop, neck anchor, face proportions, hair, glasses, and facial hair fixed.

| State | Purpose |
| --- | --- |
| `neutral` | Closed-mouth listening pose and held frames. |
| `talk` | Open-mouth speech pose. |
| `exclaim` | Rounded-mouth emphasis for the interruption. |
| `blink` | Closed eyes with the neutral mouth. |

The full set contains six master portraits and eighteen edited variants. Every image needs genuine transparency. The browser stage swaps aligned states and moves the head as one piece.

Use one subdirectory per person, with `neutral.png`, `talk.png`, `exclaim.png`, and `blink.png`. Store project-bound assets beside the eventual animation source. That location is not selected yet. Do not leave production assets only in a generator cache.

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
Preserve: the reference person's face proportions, hair, glasses, facial hair, skin tone, and distinguishing visible features
Constraints: no text, no props, no invented accessories, no extra people
```

For an expression edit, change only the mouth or eyelids. Preserve the entire remaining portrait and canvas alignment. Inspect the result against the master before using it.

## Opening picture and sound

| Time | Picture | Sound |
| --- | --- | --- |
| 00:00–00:02 | Star field and the heading THE GRAPH WAS ALREADY THERE. | A quiet cinematic rise. |
| 00:02–00:24 | Gold perspective crawl with original text from the script. | The narrator reads those exact words. |
| 00:24–00:29 | Hard cut to the flat control room. All six heads are visible. Ishan begins. | Music stops. Dialogue and room tone remain. |
| 00:29–00:37 | John T connects typed ports. Kyle draws a boundary. Graham opens the terminal. | Three short lines and small paper sounds. |
| 00:37–00:43 | A paper trait error crosses Alec's desk. It covers a coffee cup. | Brief silence, then “Oh, the humanity.” |
| 00:43–00:48 | Ryan rolls the paper aside. Graham adds the matching adapter. | Ryan's line. The music returns. |
| 00:48–00:55 | Paper graph becomes the actual composer. CUT HERE title. | The narrator establishes that pipelines worked and introduces the GDL workflow. |

These are draft picture cues. Adjust them to the recorded performances. Preserve the 55-second budget before the catalog section.

Set the crawl as live browser text. Keep the visible words identical to the narrator's words. Use CSS perspective and an explicit scene clock. The screen text must remain readable at the chosen capture size. Do not generate a flattened image of the crawl.

The compiler paper is a theatrical prop. Suggest the scale of a difficult trait error with nested type fragments. Label the exchange as dramatization. Do not pass the prop off as actual output from the current GDL build.

Give each character one compact name label on first appearance. Use the supplied screen labels until the owner provides different display names.

## Browser and capture behavior

Use one deterministic stage for the crawl and paper room. The space scene and cutout room share a scene clock. Separate layers hold the background, bodies, portrait states, props, names, and captions.

Support play, pause, replay, and seeking to an exact cue. Keep controls outside the capture area. Scene selection must reset every portrait, prop, and audio cue. Do not time the mouth motion from unrelated page timers.

Use the recorded voice timings for mouth swaps. Speech can alternate neutral and talk states at a restrained rate. Reserve exclaim for emphasis. Keep blinking occasional and deterministic.

Keep dialogue performances separate from the narrator, music, and effects. Character voices can be original performances. Reference photos supply appearance, not voice recordings.

## Asset checks

- The six identities match their labeled photos and share the same cartoon treatment.
- Every head has a complete hair silhouette and clean transparent edges.
- Each person's states share the same crop, canvas, and neck anchor.
- Mouth and blink changes preserve identity and identifying features.
- Faces remain readable at their actual screen size.
- Name labels and captions do not overlap the graph or portrait features.
- The crawl remains readable and synchronized with the narration.
- The paper gag ends before the live demonstration. Proof shots keep tool output visible.
