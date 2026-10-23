# Ryan — portrait master

Created 2026-10-22 with the built-in image-generation tool. The owner labeled the first reference “me,” identifying Ryan. That grayscale image supplies the likeness. Ishan's existing master supplies style and framing only.

Output: [neutral.png](neutral.png). This is the closed-mouth master. The remaining expression states are pending. The owner's requested color revision uses artistic warm skin tones and brown hair.

The first master was a 1254×1254 RGBA PNG with transparent margins. The color revision below supersedes its grayscale palette.

Visual inspection: the hair silhouette and ears are complete. The mouth is closed, the face is upright, and the neck anchor is centered. There are no shoulders, clothing, props, labels, or extra people. Owner likeness approval remains pending.

## Provenance

- Likeness reference: `/var/folders/gz/c44sd99n1yl7j50x8tlvz_bc0000gn/T/TemporaryItems/NSIRD_screencaptureui_bnUHBb/Screenshot 2026-10-22 at 2.15.37 PM.png`.
- Style reference: `agent-docs/video/assets/ishan/neutral.png`.
- Generator original, retained: `/Users/ryan/.codex/generated_images/01a1653f-bb39-7130-a2fc-0c9642946c92/exec-e3dc2efc-2da7-4188-9efb-baf32f44fd33.png`.
- Generation mode: built-in tool with both local reference paths and `transparent_background: true`.

## Exact generation prompt

```text
Use case: stylized-concept.
Asset type: transparent cartoon talking-head master for an original cut-paper animated technical film.
Input image roles: Image 1 is the ONLY likeness reference, explicitly labeled "me" by the user (Ryan). Image 2 is Ishan's existing cartoon portrait and is a STYLE AND FRAMING REFERENCE ONLY. Do not blend the two people's identities.
Primary request: create a recognizable cartoon portrait of Ryan from Image 1, matching the simple flat paper-cut animation treatment and relative head/neck proportions of Image 2.
Subject: Ryan, upright and nearly front-facing, friendly neutral listening expression, small CLOSED mouth, eyes open looking at the viewer.
Preserve from Image 1: his distinctive thick tousled swept-up hair and forelock, face proportions, brow shape, nose, jaw and chin silhouette, and clean-shaven to very slight stubble appearance. Do not add a beard or glasses.
Style/medium: flat 2D paper-cut cartoon, simple bold shapes, restrained dark outlines, minimal two-tone shading, subtle paper finish. Match Image 2's white oval eyes with dark pupils and its facial simplification. Keep Ryan's identity separate from Ishan's.
Color palette: the likeness reference is grayscale. Preserve a subdued grayscale palette for Ryan's hair, face and neck. Do not invent his real-world skin, hair, or eye colors.
Composition/framing: HEAD AND NECK ONLY, no shoulders, hoodie, body or clothing. Square 1254 by 1254 canvas. Complete the hair silhouette and both ears with clear transparent margins on every side. Center the neck anchor and cut the neck flat at the bottom for attachment to a separate paper body. Face upright, no selfie tilt.
Background: genuinely transparent with clean alpha, not a drawn checkerboard.
Constraints: no text, labels, logos, props, extra people or accessories. Omit the photo background entirely. Avoid realistic skin texture, glossy 3D rendering, painterly shading and detailed etched portrait rendering. Generate just one neutral master portrait, not an expression sheet.
```

## Color revision, 2026-10-22

The owner requested more color after seeing the grayscale portrait in the stage. The active master now uses artistic warm skin tones and brown hair. Its face, silhouette, expression, framing, and transparent background remain consistent with the first master. The original grayscale generator output above remains available as provenance.

- Current generator original: `/Users/ryan/.codex/generated_images/01a16525-8b4f-7f51-a81c-93491eeb5d3b/exec-bd87226f-dd4f-49e1-b393-dbdfebdfc1db.png`.
- Mode: built-in image edit with the existing Ryan master as the target and Ishan as a palette/style reference, with `transparent_background: true`.
- Saved output: [neutral.png](neutral.png), replacing the grayscale master at the owner's request.

Exact edit prompt:

```text
Use case: identity-preserve.
Asset type: transparent cartoon talking-head master for an original cut-paper animated technical film.
Input roles: Image 1 is the edit target, Ryan's existing grayscale cartoon head. Image 2 is Ishan's finished cartoon head, a palette saturation and paper-texture reference ONLY. Do not blend identities.
Primary request: colorize Ryan's existing grayscale cartoon portrait so he fits naturally beside the colorful cast. Give his face and neck warm fair peach skin, a little healthy rosy warmth in the cheeks and ears, warm medium-to-dark brown hair with lighter brown paper highlights, brown eyebrows, natural muted rose lips, and dark brown pupils. This is an artistic color choice explicitly requested by Ryan, not a claim to recover the original photo's colors.
Preserve exactly: his recognizable face, swept-up tousled hair silhouette and forelock, eyes, nose, jaw, small closed neutral mouth, ears, front-facing pose, proportions, expression, linework and subtle cut-paper texture. Keep the same image size, placement, full hair/ear margins and neck anchor.
Style: retain the existing flat 2D cut-paper cartoon style and restrained two-tone shading. Color should be warm and alive, with clear simple color shapes matching the cast. Do not make him orange or overly tan. Replace the gray skin and gray hair shading with the natural warm colors.
Background: preserve genuinely transparent background and clean alpha.
Constraints: head and neck only, no clothing, shoulders, body, new accessories, text, watermark, or additional people. Do not redraw or beautify the face, add a beard, or change the silhouette. Make only the color correction.
```
