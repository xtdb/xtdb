# init-workspace-flow state

Rosetta init-workspace-flow tracking state for the XTDB 2.x repo.
Single writer: the orchestrator.

## Phase 1 — Context [COMPLETED]

- **mode**: `install` — no `bootstrap_rosetta_files` existed before this run (`gain.json`, `dev/CONTEXT.md`, etc. were all absent).
- **plugin_active**: `false` — Rosetta is running in MCP mode (no `RUNNING AS A PLUGIN` marker; `get_context_instructions` tool present).
- **composite**: `false` — single git repository.
  It is a multi-module Gradle build, but there is one documentation root; the git submodules (`docs/shared`, `docs/lib/railroad`, `modules/datasets/lib/tsbs`) are vendored external libraries, not independent project doc roots.
- **path override**: docs root remapped to `dev/` (per `gain.json`) because `docs/` is the live Astro documentation site.

### Existing-file inventory (against bootstrap_rosetta_files, remapped to dev/)

| Rosetta file | Mapped location | Status |
| --- | --- | --- |
| gain.json | `gain.json` | CREATED (phase 1) |
| CONTEXT.md | `dev/CONTEXT.md` | pending (phase 6) |
| ARCHITECTURE.md | `dev/ARCHITECTURE.md` | pending (phase 6) |
| TODO.md | `dev/TODO.md` | pending (phase 6) |
| ASSUMPTIONS.md | `dev/ASSUMPTIONS.md` | pending (phase 6/7) |
| TECHSTACK.md | `dev/TECHSTACK.md` | pending (phase 6) |
| DEPENDENCIES.md | `dev/DEPENDENCIES.md` | pending (phase 6) |
| CODEMAP.md | `dev/CODEMAP.md` | pending (phase 3) |
| REQUIREMENTS/* | `dev/REQUIREMENTS/` | not created (no source requirements) |
| PATTERNS/* | `dev/PATTERNS/` | pending (phase 5) |
| IMPLEMENTATION.md | `dev/agents/IMPLEMENTATION.md` | pending (phase 6) |
| MEMORY.md | `dev/agents/MEMORY.md` | pending (phase 6) |

## Phase 3 — Discovery [COMPLETED]

4 parallel read-only subagents (build/topology, tech-stack/deps, architecture/domain, patterns/conventions) returned evidence-backed findings → `dev/CODEMAP.md`.

## Phase 4 — Rules [SKIPPED]

Copying Rosetta rules locally is optional and not recommended; MCP serves them on demand.

## Phase 5 — Patterns [COMPLETED]

`dev/PATTERNS/INDEX.md` + `CHANGES.md` written from `AGENTS.md` / `dev/README.adoc`, confirmed in code.

## Phase 6 — Documentation [COMPLETED]

`dev/{CONTEXT,ARCHITECTURE,TECHSTACK,DEPENDENCIES,TODO,ASSUMPTIONS}.md` and `dev/agents/{IMPLEMENTATION,MEMORY}.md` written.

## Phase 8 — Verification [COMPLETED]

All 13 bootstrap files present under `dev/`/root; none leaked into the Astro `docs/` site; git shows only new untracked files (non-destructive).

## Phase 2 — Shells [SKIPPED — plugin mode]

Rosetta is now installed as a Claude Code plugin (`rosetta:*` skills/workflows are native).
Per the `init-workspace-flow-shells` skill ("Skipped in plugin mode"), no proxy shells are written.
`gain.json` mode set to `plugin`.

## Phase 7 — Questions (HITL) [IN PROGRESS]

Open decisions surfaced to user: (1) commit dev/ Rosetta files to SCM? (2) wire `.claude/` integration shells? (3) relationship to existing dev/README + dev/doc.

## Gaps logged for Phase 7

- Confirm whether Phase 2 integration shells are wanted (writes into `.claude/`).
- Confirm whether Rosetta docs under `dev/` should be committed to SCM or stay local.
