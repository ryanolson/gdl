# John T neutral talking head

User direction: John T must wear a Canada hat. This edit adds a red Canadian baseball cap with a white maple-leaf emblem.

Mode: built-in image_gen, edit existing generated portrait; transparent_background=true.

Edit target: `/Users/ryan/.codex/generated_images/01a16525-8b4f-7f51-a81c-93491eeb5d3b/exec-8ca93029-c9f8-42c3-8eff-fef5440dc492.png` (unhatted generated master, preserved). Its source generation prompt follows the edit prompt below.

Generated edit original: `/Users/ryan/.codex/generated_images/01a16542-c032-75d2-9e74-7022a73d1119/exec-13e674d9-68aa-4d78-aa5f-5d37b9a88706.png` (preserved).

Saved asset: `neutral.png`.

Actual PNG: 1254 × 1254 pixels, 8-bit RGBA (PNG IHDR color type 6).

Visual QA: red forward-facing cap with centered white maple leaf; complete hat silhouette in frame; transparent background; face, glasses, ears, neutral closed mouth, neck position, and paper-cartoon style visually preserved. No shoulders, clothes, extra props, or text.

Exact edit prompt:

```text
Use case: precise-object-edit
Asset type: transparent paper-cartoon talking-head sprite.
Edit request: Add a red Canadian baseball cap with a clear centered white maple-leaf emblem to John T in this existing portrait. This is the only change. The user explicitly requested that John T wear a Canada hat.
Edit target: the supplied generated John T portrait. Treat it as a locked existing asset, not a new portrait.
Invariants: Preserve his exact existing face, facial proportions, slim rectangular black glasses, eyes and pupils, eyebrows, ears, skin, nose, friendly closed mouth, and chin. Preserve his head and neck position, head scale, front-facing upright pose, square canvas dimensions and transparent alpha. Preserve the same flat 2D textured paper-cartoon rendering and fine dark outlines.
Hat: a forward-facing red baseball cap, correctly seated on his head, with a crisp white maple-leaf emblem centered on its crown. The cap may cover some or all of his hair. Keep complete cap silhouette in frame with transparent margin; do not crop cap or ears. Do not cast a heavy shadow over his face or obscure his glasses or eyes.
Background: genuinely transparent with alpha. Preserve existing transparency. No painted checkerboard.
Avoid: any face redesign, expression change, open mouth, shoulders, clothes, other props, extra accessories, text, words, watermark, extra characters.
```

Original master prompt:

```text
Use case: stylized-concept. Asset type: neutral master cartoon talking head for an original cut-paper technical comedy. Input image 1 is ONLY the named person's likeness reference. Input image 2 is ONLY a rendering-style and composition reference (the Ishan cartoon), not an identity reference. Match the Ishan asset's original flat 2D paper-cut cartoon treatment: simple white oval eyes with dark pupils, restrained dark outlines, clean large graphic shapes, subtle paper texture and minimal flat shading. Preserve the identity of image 1 and do not blend it with Ishan. Friendly NEUTRAL listening expression with a small CLOSED mouth and open eyes. Upright, nearly front-facing HEAD AND NECK ONLY, no shoulders or body, complete hair silhouette, centered flat-bottom neck anchor, generous margin around the head. Square 1254 by 1254 composition matching the reference's overall head scale. Genuinely transparent background with clean alpha, no drawn checkerboard. No text, name labels, props, extra people, new accessories, glossy 3D, photographic texture, or detailed poster rendering. Subject: John T, the person labeled John T by the user. Preserve his slender oval face, light skin, short dark-brown hair with a slightly uneven short fringe, slim rectangular dark-rim glasses, distinctive nose, prominent ears, and clean-shaven face. Convert the smile to a small friendly CLOSED-mouth resting smile suitable for a neutral animation master. Keep his identifying glasses but omit the blue lens glare and photographic color cast. Omit the plants, raised hand, patterned shirt, photo UI text, and entire background.
```
