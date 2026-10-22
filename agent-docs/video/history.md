# Historical sources for the film

## 2026-10-22 addendum: supplied Slack excerpts

Source: [user-supplied research attachment](</Users/ryan/.codex/attachments/e1160eee-2852-4621-82c3-0b4e08829df7/Pasted text.txt>).

The quotations, dates, and Slack links below are transcribed from that attachment. The Slack links were not opened. Their contents were not independently verified.

The selected excerpts document graph ambitions, existing type constraints, a working Rust pipeline, and difficulty with generic error messages. They do not establish why any earlier effort stopped.

## Selected excerpts

### Kyle Kranen — November 20, 2024

> we need something path or graph based

[Supplied message link](https://nvidia.enterprise.slack.com/archives/C07UMGUN6RL/p1732126813856189)

The attachment places this message in a group DM with John. It supplies a short statement of the graph idea.

### John Thomson — December 2, 2024

> only allow links if the output matches the input.

[Supplied message link](https://nvidia.enterprise.slack.com/archives/D06K5JK355J/p1733177106871489)

This excerpt comes from John's question about graph validation in nova-init. He described input and output types on each class. He also questioned the extensibility of that approach. This quotation concerns nova-init, not the Rust pipeline.

### Ryan Olson — December 5, 2024

> but this code works for distributed llm with trtllm

[Supplied message link](https://nvidia.enterprise.slack.com/archives/C08485FQKQ9/p1733422276643959)

The attachment associates this message with Rust pipeline code that uses `.link(preprocessor.forward_edge())` and `.link(backend.backward_edge())`. Ryan explicitly reported that the code worked.

### Graham King — December 11, 2024

> Hey Ryan. I'm stuck on pre/post processing types.

[Supplied message link](https://nvidia.enterprise.slack.com/archives/D07Q2B75GPM/p1733941036193109)

The attachment identifies the edge types as `ChatCompletionRequest -> BackendInput -> LLMEngineOutput`. This message reports difficulty with a concrete integration.

### Graham King — December 12, 2024

> my love/hate thing with all the generics. At first it was annoying, and the error messages are fearsome, but once I figured it out...

[Supplied message link](https://nvidia.enterprise.slack.com/archives/D07Q2B75GPM/p1734042068535599)

The qualification remains part of the quotation. The supplied excerpt ends with an ellipsis, so it does not supply Graham's full conclusion.

### Graham King — December 13, 2024

> Hah, no way! I have it. I copied from apps tensortrt demo, I needed `Backend`

[Supplied message link](https://nvidia.enterprise.slack.com/archives/D07Q2B75GPM/p1734127680481889)

This follow-up reports success. A sequence that quotes the earlier difficulty must preserve this outcome.

## Source boundaries

- The attachment states a scope of November 2024 through March 2025.
- An April 21, 2025 statement attributed to Ryan appears through Alec's quotation. It falls outside that scope and is excluded here.
- An October 2025 statement also falls outside that scope and is excluded here.
- The attachment labels explanations about parallel efforts, changing priorities, and why an approach faded as inference. Those explanations are excluded from the film's historical conclusions.
- The excerpts do not establish that the Rust pipeline lacked type constraints or failed to work.
- The excerpts do not establish that Dynamo's current distributed serving depends on a full port to GDL.

## Cast and dialogue

The requested talking heads are Ishan, Alec, John T, Kyle, Graham, and Ryan. The portrait plan uses reference photos for cartoon likenesses.

All proposed character dialogue is fictional unless it reproduces an attributed quotation from the supplied source. Fictional lines do not establish anyone's personality, beliefs, or historical actions.
