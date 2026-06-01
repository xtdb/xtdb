# PATTERNS — INDEX

Navigational index of the coding/architectural patterns this codebase follows.
Status: current.
The patterns are stated authoritatively in `AGENTS.md` and `dev/README.adoc` (and `dev/GIT.adoc` for git); this file is a one-line-each pointer so an agent knows what exists and where to read it — it does not restate the rationale or cite code sites (those drift).

## Architectural

- **Make illegal states unrepresentable** (+ corollary: no hack values) → `dev/README.adoc`, "General Coding Principles".
- **Storage / State / Services layering** (functional core, imperative shell) → `dev/README.adoc`; modelled in `dev/ARCHITECTURE.md` and `dev/doc/db.allium`.
- **Tidy-first** (separate equivalence changes from behaviour changes) → `AGENTS.md`, `dev/README.adoc`.
- **Intentional defaults** (defaults only where they're the overwhelming norm) → `dev/README.adoc`.
- **Coordination-free services** (deterministic + idempotent, e.g. compaction) → `dev/doc/compaction.allium`.

## Coding conventions

- **Errors via `xtdb.error` / `Anomaly`** (no raw exceptions; `incorrect`/`unsupported`/`fault`/…) → `AGENTS.md` ("Errors"), `dev/README.adoc`.
- **Comments explain why, not what** → `dev/README.adoc` ("Comments").
- **Sentence-per-line** in docs and commit bodies → `AGENTS.md`.
- **Clojure↔Kotlin interop** (`.getX` for Kotlin `val`s; type-hint at the destructuring site) → `AGENTS.md`.
- **Conventional commits with scopes** (`!` only for user-facing breaks) → `AGENTS.md`.

## Testing

- Test locations, tasks, tags, property testing, allium specs → `AGENTS.md` ("Running tests"), `dev/README.adoc` ("Testing").
- Observe real phenomena, not mock calls; reserve mocks for injecting failures → team convention (see test suites).

## Workflow

- **Ship / Show / Ask** → `dev/README.adoc` ("Git"), `dev/GIT.adoc`.
- Commit/PR/issue prose via the chalk skills.

See `dev/PATTERNS/CHANGES.md` for the change log.
