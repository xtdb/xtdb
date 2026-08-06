# Positioning

How we talk about *why* XTDB exists, as distinct from what it does.

Read this before writing anything persuasive — a landing page, the `about/` pages, a conference abstract, the rationale at the top of a PR.
For architecture, read `dev/doc/`.
For the published form of the argument, read `src/content/docs/about/mission.md`; this is the fuller framing behind it, and not all of it has a home on the site.

Drawn from the "Stop Fighting Time" talk and the sales-enablement deck.
Client names, case-study detail and commercial material are deliberately omitted — this file is in a public repo, and none of that is needed to get the framing right.

**tl;dr**

- **Mission**: reduce the time and money it takes for organisations with business-critical, time-oriented requirements to safely build and maintain systems of record.
- **One line**: XTDB helps when **time is complex and correctness matters**.
- **The wedge is "both, not either"** — update-in-place ease *and* a bitemporal safety net. This is what separates us from an RDBMS and from event-sourcing; lead with it.
- **The cost we attack is maintenance, not the initial build** — temporal workarounds leak filters into every query and join, forever.
- **Adoption is incremental**: be the default for *new* work, augment what exists. Never a rip-and-replace pitch.

## Mission

> to reduce the time and money it takes
> for organisations with business-critical, time-oriented requirements
> to safely build and maintain systems of record

- **The one-line version**: XTDB helps when time is complex and correctness matters.

## The wedge: both, not either

- **The pitch is not "immutability good, mutation bad".** It's two things held together:
  - the **ease and performance** of a traditional update-in-place database, for everyday transactions and queries, **and**
  - the **safety net of bitemporality**, for when you need it.
- **Most temporal designs make you choose** — model history explicitly everywhere and pay for it in every query, or give it up. The claim is that this is a false choice.
- **Lead with this.** It's what separates XTDB from both a conventional RDBMS and from event-sourcing.

## Pain, workaround, cost

- **Temporal requirements turn up constantly**, usually without being recognised as temporal:
  - "Could you just show us the edit history of this company profile?"
  - "I want the user to be able to delete this post on their public profile, but still see its performance on their reporting dashboard."
  - "Oh btw, I moved house last month."
  - "Could you schedule this marketing promotion for next month?"
  - "An upstream system was running late, we're going to need to recalculate the end-of-month reporting."
  - "On what data did we make that decision?"
  - "But I didn't mean to delete that data!" … "what do you mean, we can't restore the backup?"
- **Each has a conventional answer, and each answer costs something** — soft deletes, manually maintained effective-from/effective-to (SCD type 2), history tables plus triggers (SCD type 4), bespoke versioning, denormalisation, ad hoc amending of data and reports.
  - **And when nobody modelled it in up front**: WAL backups and checkpoints, CDC into a data lake, storage-level snapshots.
- **The cost is maintenance, not the build.** Temporal filters leak into every subsequent query and join. `WHERE deleted_at IS NULL` everywhere is the small version; the same thing across a join is the version that bites.
- **Stated as a contract, those "best practices" say**: `INSERT` is still an insert, `UPDATE` becomes update-plus-insert, `DELETE` becomes an update, `ERASE` becomes a delete, every query needs its temporal filters, and analytics needs a separate OLAP system, an ETL, and a loss of consistency.

> Any sufficiently complicated data system contains an ad hoc, informally-specified, bug-ridden, slow implementation of half of a bitemporal database.
>
> — "Henderson's Tenth Law", with apologies to Greenspun

## In XTDB

- **The DML means what it says** — `INSERT` is `INSERT`, `UPDATE` is `UPDATE`, `DELETE` is `DELETE`, `ERASE` is `ERASE`.
- **Every entity gets its own bitemporal history**, without being modelled for it.
- **Queries are "as of now" by default**, with history available when you ask.
- **One consistent system for OLTP and OLAP** — no ETL.

## What time travel actually unlocks

Worth being concrete; "time travel" alone sounds like a novelty.

- **Correct the past** — errors, late-arriving data, system failures — then see the data both with and without the corrections.
- **Every query runs at a basis**, a reproducible snapshot of the world.
- **Back in time**: re-run a query at an earlier basis and get the same answers, guaranteed.
  - "Exactly what data did my AI agent base that decision on?"
  - "Why were these figures wrong?"
- **Stop time**: run many queries at one basis for a consistent set of results — a dashboard, a report, a training set.
- **Backtesting**: run a *new* model at an *old* basis, to see how it would have performed.
- **The differentiator**: none of it needs proactive, explicit, scheduled snapshotting.

## Adoption

- **The first objection is always "database migrations are massive, costly projects".** The answer is that adoption is incremental — and the framing matters:
  - **Be the default for new work** — new projects, and extensions to existing systems — rather than ripping out an established database.
  - **Augment what's already there**, through connectors and ecosystem compatibility.
  - **No big bang: every increment adds value.**
- **Concretely, an increment is**: add a data source (Postgres, Kafka, direct SQL DML), and add compute sized to that application's own workload.
- **Federation (v2.1) and external sources (v2.2) are what make this real** — they let XTDB sit as a component in a data mesh, rather than as something everything else must move onto first.

## The other standing objection

- **"Other databases have, or are getting, temporal functionality — and why trust a new database anyway?"** Two answers:
  - **Cloud-native**: built on battle-tested components, inheriting their scalability, availability and durability, and keeping storage costs low.
  - **Open**: MPL-licensed and on GitHub, Apache Arrow as the storage format, and compatible with PostgreSQL queries, drivers and tooling — so no lock-in.
