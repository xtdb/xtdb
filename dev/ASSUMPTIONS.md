# ASSUMPTIONS

Assumptions and unknowns recorded during Rosetta initialisation, for the user to confirm or correct.
Status: open.

## Assumptions made during init

- **Docs relocated to `dev/`** — because `docs/` is the live Astro documentation site, all Rosetta-managed docs were written under `dev/` (and working files under `dev/agents/`), recorded in `gain.json`.
  Assumed acceptable per the user's instruction "use /dev folder for Rosetta docs".
- **Mode = install** — no prior `bootstrap_rosetta_files` existed.
- **composite = false** — single git repository with one documentation root; submodules are vendored externals.
- **Phase 4 (copy rules) skipped** — optional and not recommended; Rosetta serves rules on demand via MCP.
- Content of `dev/CONTEXT.md`, `ARCHITECTURE.md`, `TECHSTACK.md`, `DEPENDENCIES.md` was distilled from existing authoritative docs (`AGENTS.md`, `dev/README.adoc`, `dev/doc/*`) and a read-only code-discovery pass; library versions are a snapshot of `gradle/libs.versions.toml` at init time.

## Open questions (for Phase 7 HITL)

1. Commit the `dev/`-rooted Rosetta files to SCM, or keep them local / git-ignored?
2. Wire Rosetta integration shells into `.claude/` (Phase 2), or leave Rosetta MCP-only?
3. Are `dev/CONTEXT.md` / `dev/ARCHITECTURE.md` welcome alongside the existing `dev/README.adoc` + `dev/doc/*.adoc`, or should they instead point to / be merged with those?

## Unknowns / not verified

- GC algorithm details (not deep-read).
- Full logical-plan optimiser rule set.
