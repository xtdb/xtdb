# Agents

NOTE to humans: keep this file to instructions for AI agents; if it'd be useful for humans, add it to the developer documentation instead and point the agents to it.

Interpret MUST, MUST NOT, SHOULD, SHOULD NOT, MAY, etc. per RFC 2119.

See developer documentation in `/dev` in this repo — @dev/CODING.adoc carries the conventions your code is held to, and `dev/README.adoc` the human-facing setup, tooling and release material.
`dev/doc/*.adoc` describe the high level architecture of XTDB - read those too.

For editing or adding pages to the user-facing documentation site (`docs/src/content/docs/`), see the "Writing docs" section in `docs/README.md` — it covers the Diataxis mapping, voice, changelog conventions, and a few patterns that trip agents up (properties vs mechanism, per-database scoping, theme-neutral diagrams, mining commits for context).

We develop using 'tidy-first' methodology - endeavouring to separate 'equivalence' changes (changes which do not affect runtime behaviour, changes which increase our options) from changes that advance the behaviour of XT.
For example, even when we're working on a feature branch, we will often separate a tidying change in a separate commit and cherry-pick it onto `main`, so that the resulting PR is easier to review.

We take great inspiration from the principle of 'making illegal states unrepresentable' - prefer type systems, data structures, and APIs that prevent invalid states at compile time rather than requiring runtime validation.

* This is a Gradle project - you MUST NOT use Clojure CLI tools to run code within the project; they will not bring in the dependencies correctly.
* Modules are named `xtdb-<directory>` - e.g. `:xtdb-core`, `:modules:xtdb-kafka` - so that the Maven artifacts have the `xtdb-` prefix.
* We use conventional commits for commit messages. Common prefixes: `feat:`, `fix:`, `refactor:`, `tidy:`, `build:`, `test:`, `dev:`, `ai:`.
  * Use sub-tags/scopes in parentheses where relevant, e.g. `fix(ee):`, `feat(sql):`, `refactor(logical-plan):`.
  * `feat:` is for **externally-visible** functionality only — new SQL syntax, a new config YAML key, new pgwire behaviour, a new public Java/Kotlin API that users call.
    A new internal abstraction, seam or behaviour-preserving reorg is `refactor:` (or `tidy:` for pure equivalence changes), however large it is.
    Ask "can a user see or do something new because of this change?"; if not, it isn't `feat:`.
  * Indicate breaking changes with `!`, e.g. `tidy!:`.
    `!` only applies to **user-facing** API changes (SQL syntax, pgwire protocol, config YAML, public Java/Kotlin API).
    Internal refactors (e.g. changing internal types like `TransactionResult`, `Watchers`) are not breaking even if the types are technically `public` in Kotlin.
    The surface MUST also have **shipped in a release**.
    Changing a public API that was added but never released breaks no existing user, so it's a plain `fix:`/`feat:` — no `!`, and no `breaking change` label.
  * Where chalk is available you MUST write commit messages with `chalk:commit` — see the `xtdb-github` skill.
* For file operations (reading, searching, editing, writing), you SHOULD use the built-in tools (`Read`, `Edit`, `Write`, `Glob`, `Grep`).
* For REPL evaluation, use the `clj-nrepl-eval` command via Bash or the `/clojure-eval` skill.

## Stakes

XTDB is deployed as critical infrastructure at national scale.
That is the actual operating context, not a device to make you more careful — a bad commit that reaches production takes down systems people depend on.

What that changes about how you work:

* When you change an interface, grep for every consumer across the whole project, not just the module the interface lives in.
* "It compiles in the module I changed" is not verification.
* When in doubt, verify more rather than less.

## Allium specs

`.allium` files are behavioural specifications of XTDB's subsystems — entities, rules, invariants, contracts and surfaces, plus the rationale and open questions that the code alone doesn't carry.

Before you make an architectural claim about one of the areas below, or change the code that implements it, you MUST read the corresponding spec.
Where the spec and the code disagree, say so rather than silently picking one: some specs deliberately run ahead of the implementation, and mark themselves as such (`db.allium`'s leader-pipelining section, for instance).

Specs live in two directories, `allium/` and `dev/doc/` — glob `**/*.allium` rather than assuming a single location.
Use the `allium:tend` skill to edit a spec and `allium:weed` to check a spec against the code.

You MUST also load `chalk:allium-voice` before writing or editing any spec prose — `allium:tend` does not load it for you, so the caller has to.
A spec states the aim, and what it does not state, it excludes: an obligation named after what it is not, or prose arguing for this design over the one that was rejected, is misfiled.
That commentary belongs in the commit body and the PR, which are read once around the change; a spec is read long after, by someone who doesn't care what it might have been.

| Spec | Covers |
| --- | --- |
| `allium/live-index.allium` | In-memory staging of transactions before block flush — `LiveIndex`/`LiveTable`, transaction lifecycle, snapshot visibility, and the leader's resolve/append/consume-back/apply pipeline. |
| `allium/database-lifecycle.allium` | The lifecycle of a database on one node — how it becomes a member of the node's set, how it stops being one, and what resolves by name in between. Node-level; treats a running database as opaque. |
| `allium/log-processor-lifecycle.allium` | Per-database leader election in the `LogProcessor` — the Following/Leading state machine, the term fence that keeps one confirmed leader per database, and the five steps a promotion runs off the transport's serialization point. |
| `allium/memory-hash-trie.allium` | The immutable in-memory hash trie indexing rows by IID within a `LiveTable` — bucketing, leaf ordering, log compaction, splitting. |
| `dev/doc/db.allium` | The processing model of one database end to end — submit → log processing → block flush → query, the source/replica log message types, the block and table catalogs, and the compaction message flow. |
| `dev/doc/tx.allium` | The connection-level transaction model sitting above `db.allium` — begin/buffer/commit, access-mode resolution, read basis, and pgwire/ADBC frontend parity. |
| `dev/doc/trie-cat.allium` | The trie catalog — per-table inventory of immutable trie snapshots, the nascent/live/garbage state machine, and supersession by compaction output. |
| `dev/doc/compaction.allium` | Coordination-free compaction — deterministic job selection and the merge algorithm. The message flow around it lives in `db.allium`. |
| `dev/doc/gc.allium` | Trie garbage collection — leader-only, signalled at block boundaries, deleting superseded data/meta files and publishing those deletions atomically to the replica log. |
| `dev/doc/block-gc.allium` | Garbage collection of superseded block-catalog and per-table block files — leader-only, and needs no replica-log coordination because block files are read only at startup/catch-up. |

## Definition of Done (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

* You MUST include tests for new/changed functionality.
* You MUST run tests locally to verify they pass, per the `xtdb-testing` skill.
* You MUST update the Allium specs if you've made changes in the areas they cover — see [Allium specs](#allium-specs) for the map.
* The full test suite MUST pass (`./gradlew test`).
  If you've affected any integration tests (e.g. Kafka, remote storage), you MUST also run `./gradlew integration-test`.
  If you've touched indexing, compaction or GC, you MUST also run `./gradlew property-test` — those are the subsystems the simulation tests cover, and `./gradlew test` cannot reach them.
  CI will run integration and property tests regardless, but `./gradlew test` is the minimum.
  All tests pass on `main`, so a failure is yours — see [Running tests](#running-tests) for how to handle one.
* There MUST NOT be any reflection or boxed math warnings.
* For show/ask changes, you MUST run a code-review pass over the diff before raising the commit/PR — see the code-review note under `== Git` in @dev/CODING.adoc (ship changes are exempt).
  That pass MUST cover the diff's comments against `chalk:code-comments`, and any spec prose against `chalk:allium-voice`, not only its code.
* Verify: all changes committed AND pushed
* Hand off: provide context for next session

For user-visible features:
* You MUST update relevant docs in `/dev/doc/` or user-facing documentation when implementing new features or changing existing behaviour.

## Style

You MUST use sentence-per-line in documentation files - this makes diffs cleaner and easier to review.
That covers `.allium` prose too, overriding the column wrap the existing specs use — a spec is reviewed as a diff like any other file here.
Existing specs converge as they're edited; don't reflow a file you aren't otherwise changing.
For commit messages, defer to the chalk commit skill's line-break convention where it's loaded — see the `xtdb-github` skill.

For comments, you MUST load the `chalk:code-comments` skill — early in any session that will write or change code, and again when reviewing a diff.
The "Comments" section in @dev/CODING.adoc carries why that timing matters, and the XTDB-specific instances.

For test comments specifically: the test name and assertion ARE the documentation.
Don't add a comment restating intent or citing an issue number when the test name already encodes them — e.g. a test named `test-foo-bug-1234` doesn't need a comment citing #1234.
Trust the test body to show the rest.

For errors, see the "Errors" section in @dev/CODING.adoc — use `xtdb.error`, not raw Java exceptions.

## Object roles and boundaries

You MUST invoke the `xtdb-object-boundaries` skill when planning a change, whenever you decide where something belongs, and before you review a diff that does.
That covers:

* deciding which type holds a piece of state, whether something is one object or two, or which object a method goes on;
* adding or moving a class, an interface, an object or a namespace;
* adding a field that outlives a single call, or a coroutine scope;
* splitting a type up, or merging two;
* threading a value through a new parameter — often the tell that the boundary is in the wrong place;
* a `Map<Id, State>` sitting beside the objects it identifies, or a field only set in some states.

Adding a method to the type it obviously belongs on is not one of these; moving one between types is.

A plan MUST name the role of every object it adds, moves, or changes the shape of.
Naming it is what makes the identification checkable: a plan that considered the roles silently is indistinguishable from one that skipped them, and the plan is the cheapest place to be told the answer is wrong.

XTDB is a functional core with an imperative shell, and every object is an Active Object, a passive object or a Monitor Object.
The skill carries the three definitions, the test that tells them apart, and the rule that one object holds one role.
It also covers the state each object owns — what counts as one value, whether it is an atom or a transient, and which region and which writer it belongs to.

## Running tests

Before you run a test, or delegate a test run to a sub-agent, you MUST invoke the `xtdb-testing` skill.

It is the single home for every XTDB-specific testing rule and mechanism — delegation to the `gradle-tests` agent, the mid-run edit freeze, test tasks and filters, iteration counts, the simulation tests that `./gradlew test` cannot reach, diagnosing a failure, and regenerating arrow-edn golden fixtures.
Do not reconstruct any of it from memory: the `gradle-tests` agent is generic (from the `xtdb/claude-plugins` marketplace) and carries none of this knowledge itself, so it is yours to pass on.

## Rebasing and merging

Before you rebase a branch, resolve a conflict, or merge a branch back into `main`, you MUST invoke the `xtdb-git` skill.

It carries why history stays linear, how to resolve a conflict without silently reverting a fix that landed on `main` after the branch forked, and the three merge patterns.
Ship/show/ask and the code-review pass live in @dev/CODING.adoc; commit messages are `chalk:commit`'s.

## GitHub issues, PRs and the project board

Before you open an issue or PR, add a card to the board, set a milestone or a label, or pick up a card, you MUST invoke the `xtdb-github` skill.

It is the single home for the GitHub conventions in this repo — what goes on the board and what inherits it through an issue, sub-issue parenting, the two tests a milestone has to pass, the label conventions, the assign-and-set-`Status` move when you pick a card up, the cached project and field IDs, and how to drive all of it through chalk.
Do not reconstruct any of it from memory, and do not guess an ID.
