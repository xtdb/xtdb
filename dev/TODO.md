# TODO

Improvements and larger suggestions surfaced during Rosetta initialisation.
Status: open.
This is a seed list from the init pass, not an authoritative backlog — issues are tracked on the xtdb org "2.x" project board (see `AGENTS.md`).

## Documentation / init follow-ups

- Decide whether the Rosetta docs under `dev/` are committed to SCM or kept local (see `dev/ASSUMPTIONS.md`).
- Decide whether to wire Rosetta integration shells into `.claude/` (Phase 2 — deferred, pending user confirmation).
- Cross-link the existing `dev/doc/*.adoc` deep-dives from `dev/ARCHITECTURE.md` once stable.

## Areas flagged for deeper documentation (from discovery)

- Garbage-collection algorithm: `gc.allium` / `block-gc.allium` exist but were not deep-read during init.
- Logical-plan optimiser: full rule set and interactions not exhaustively captured.
- Expression-compilation kernels: optimisation paths in `expression.clj` warrant a focused write-up.
