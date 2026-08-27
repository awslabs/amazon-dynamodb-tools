# AI lint

On-demand, **AI-driven** checks for invariants that are easy to verify by reading
the code but very hard (or impossible) to express as a deterministic unit test —
e.g. "three sources must all describe the same IAM permissions", or "every command
that touches DynamoDB must be rate limited."

## How it works

There is no runner script. The "engine" is Claude (Claude Code). Each rule is a
markdown file under `rules/` describing **one** invariant: what to check, where to
look, and how to report. To run the lint, ask Claude:

> Run the AI lint (see `ai_lint/`).

Claude reads every `rules/*.md`, inspects the current code with its normal file
tools (glob/grep/read — so rules that say "discover all verbs" actually do), and
prints per-rule findings.

Run one rule by naming it, e.g. "run the `commands-rate-limited` AI lint rule."

## This is advisory, not a gate

An AI check is non-deterministic and network-bound. It is **not** part of
`make test` and must never block a merge — findings are leads for a human to
confirm, not build failures.

A false alarm isn't a failure of the tool — it's feedback. If a rule flags
something that turns out to be fine, that usually means the rule prose is
ambiguous or missing an accepted case; tighten the `rules/*.md` so the next run is
sharper. The rules are expected to improve over time this way, so err toward
surfacing a questionable case rather than staying silent.

## Adding a rule

Drop a new `rules/<name>.md`. One rule per file (keeps merges clean — new rules
never collide). Each file should cover:

- **Why it can't be a normal unit test** (one line).
- **Scope** — which files/dirs, and *discover dynamically* rather than hard-coding
  lists when new instances (verbs, generators, …) could be added later.
- **The invariant** — precisely what must hold.
- **What to check** and **how to report** (state what was verified even when
  clean, so a pass is trustworthy), plus which cases are explicitly acceptable so
  the rule stays low-noise.

See the existing rules for the shape.
