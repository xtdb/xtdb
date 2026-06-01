# MEMORY

Brief root causes of errors/mistakes and what worked or didn't, for future agents.
Status: current.

## Init-time learnings

- **`docs/` is the live Astro documentation site**, not a generic docs folder — never write Rosetta `docs/*` files there.
  Rosetta docs live under `dev/` in this repo (see `gain.json`).
- **Rosetta MCP retrieval is keyword-matched**: `query_instructions` returns full content only for narrow queries; broad ones return a listing only.
  Prefer tight, distinctive queries.
- **Authoritative existing docs** to lean on rather than re-derive: `AGENTS.md`, `dev/README.adoc`, `dev/GIT.adoc`, `dev/doc/*.adoc`, `dev/doc/*.allium`.
- **Do not run tests directly** — delegate to the `gradle-tests` agent; Clojure namespaces use underscores in `--tests` filters.
