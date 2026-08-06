# Agents

NOTE to humans: keep this file to instructions for AI agents; if it'd be useful for humans, add it to the developer documentation instead and point the agents to it.

Interpret MUST, MUST NOT, SHOULD, SHOULD NOT, MAY, etc. per RFC 2119.

See developer documentation in `/dev` in this repo, particularly @dev/README.adoc and @dev/GIT.adoc.
`dev/doc/*.adoc` describe the high level architecture of XTDB - read those too.

For editing or adding pages to the user-facing documentation site (`docs/src/content/docs/`), see the "Writing docs" section in @docs/README.md — it covers the Diataxis mapping, voice, changelog conventions, and a few patterns that trip agents up (properties vs mechanism, per-database scoping, theme-neutral diagrams, mining commits for context).

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
  * You MUST use the commit skill for writing commit messages.
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

| Spec | Covers |
| --- | --- |
| `allium/live-index.allium` | In-memory staging of transactions before block flush — `LiveIndex`/`LiveTable`, transaction lifecycle, snapshot visibility, and the leader's resolve/append/consume-back/apply pipeline. |
| `allium/log-processor-lifecycle.allium` | Per-database leader election in the `LogProcessor` — the Following/Prepared/Leading state machine, the term fence that keeps one confirmed leader per database, and the split between launching a transition and committing the role. |
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
* For show/ask changes, you MUST run a code-review pass over the diff before raising the commit/PR — see the code-review note under `=== Git` in @dev/README.adoc (ship changes are exempt).
* Verify: all changes committed AND pushed
  You MUST use the commit skill to create commit messages.
* Hand off: provide context for next session

For user-visible features:
* You MUST update relevant docs in `/dev/doc/` or user-facing documentation when implementing new features or changing existing behaviour.

## Style

You MUST use sentence-per-line in documentation files - this makes diffs cleaner and easier to review.
For commit messages, defer to the chalk commit skill's line-break convention, if loaded.
You MUST use the commit skill to create commit messages.

For comments, see the "Comments" section in @dev/README.adoc - focus on the 'why', not the 'what'.

For test comments specifically: the test name and assertion ARE the documentation.
Don't add a comment restating intent or citing an issue number when the test name already encodes them — e.g. a test named `test-foo-bug-1234` doesn't need a comment citing #1234.
Trust the test body to show the rest.

For errors, see the "Errors" section in @dev/README.adoc — use `xtdb.error`, not raw Java exceptions.

## Running tests

Before you run a test, or delegate a test run to a sub-agent, you MUST invoke the `xtdb-testing` skill.

It is the single home for every XTDB-specific testing rule and mechanism — delegation to the `gradle-tests` agent, serialising Gradle runs across worktrees, test tasks and filters, iteration counts, the simulation tests that `./gradlew test` cannot reach, diagnosing a failure, and regenerating arrow-edn golden fixtures.
Do not reconstruct any of it from memory: the `gradle-tests` agent is generic (from the `xtdb/claude-plugins` marketplace) and carries none of this knowledge itself, so it is yours to pass on.

## GitHub project board, milestones, labels

XTDB 2.x work is tracked on the [xtdb org "2.x" project board](https://github.com/orgs/xtdb/projects/13), with each release cut against a milestone named `2-NEXT` (which gets renamed to the release version when it ships — so the milestone *name* is stable but its number/ID changes).

The 2.x board is not the org's only project, and some work is deliberately tracked on another one.
"Not on the 2.x board" therefore does not mean "untracked".
Before adding a card on the grounds that an issue looks orphaned, check which projects already claim it:

```bash
gh api graphql -f query='query { repository(owner:"xtdb",name:"xtdb"){
  issue(number:NNNN){ projectItems(first:10, includeArchived:true){
    nodes{ isArchived project{ number title } } } } }'
```

Not all of those projects are public, so this file names none of them and you MUST NOT record their names, numbers or contents here.
Treat a hit from another project as "already tracked, leave it alone".

### What goes where

When you open an issue *or* PR, work out whether it's standalone or part of a surrounding issue, then:

- **There's a surrounding issue** (the PR closes, advances, or is otherwise scoped by an open issue): the **issue** carries the board card and, if the work is end-user-visible, the milestone.
  The PR does not go on the board and does not get a milestone — it inherits them through the issue.
- **It's a standalone PR** (no surrounding issue, e.g. a small fix, cleanup, or dependency bump worth noting in release notes): the **PR** goes on the board and on the milestone directly.

This mirrors how the release notes are written — one entry per issue-or-standalone-PR, never both.
Applies to docs-only and meta/repo-admin work too (1.x work is the only category that doesn't go on the 2.x board, and there's very little of that these days).

Board `Status` is set automatically on item creation — you don't need to manage it.
`Stream` is preferable to have set, but don't make one up: set it when the right category is obvious, otherwise ask, or leave it blank and let a human classify it.
Note that a sub-issue does NOT inherit its parent's stream — the parent's category says nothing about a child whose subject is something else — so "obvious from the parent" is not obvious.
When that's the only reason you'd have a candidate, ask.

### Sub-issues

A sub-issue's parent SHOULD be its natural surrounding issue — the prerequisite, or the closest piece of work that explains why this matters — not automatically the top-level umbrella.
Before filing one, ask which open issue creates the conditions for this work; that's the parent.

Nesting two or three deep is fine and often right: umbrella → migration → split-out.
Flattening everything onto the umbrella out of habit loses the dependency structure, which is the thing that makes "open and un-blocked" a usable queue.

### Milestones

A milestone is the release-notes scope, so it carries a narrower test than the board does: set one only when the work is **end-user-visible**.
Internal refactors, module-author-facing SPI tightening, test-infra and infrastructure cleanup go on the board with an appropriate `Stream` but MUST NOT go on `2-NEXT` or any other milestone, however large they are.
End users don't read about interface tightening; putting it on the milestone clutters the release notes with internal noise.

The open milestone is always named `2-NEXT`.
Look up its current REST number by name+state rather than caching it:

```bash
gh api '/repos/xtdb/xtdb/milestones?state=open' --jq '.[] | select(.title=="2-NEXT") | .number'
```

Set it on an issue or PR with `gh issue edit N --milestone 2-NEXT` / `gh pr edit N --milestone 2-NEXT`.

### Labels

We don't make heavy use of labels, but two conventions matter for release notes:

- **`breaking change`** (note: space, not hyphen): apply to any issue/PR that's a user-impacting breaking change.
  Same scope as the commit-message `!` rule documented earlier in this file — SQL syntax, pgwire protocol, config YAML, public Java/Kotlin API.
  Internal refactors don't count.
- **Component labels**: long-tailed set of area tags (`sql`, `pgwire`, `kafka`, `compactor`, `indexing`, `logical-plan`, `expression engine`, `xtql`, `docker`, `docs`, `dev-experience`, `performance`, etc.).
  Apply when a single area is obviously the subject.
  Fetch the current list with `gh api '/repos/xtdb/xtdb/labels?per_page=100' --jq '.[].name'` rather than guessing.

### Assignment

`@me` iff you're *about to work on it* — per link:dev/GIT.adoc[] the assignee is whoever is currently responsible for moving the item forward.
The chalk `github` agent already assigns `@me` when creating a chalk comment or a PR; if you're creating an issue or PR that you're not starting immediately, leave it unassigned.

### IDs (so you don't have to look them up)

The 2.x board's IDs are stable — cached here to avoid re-fetching each session.
The `2-NEXT` milestone number is *not* cached because it changes on release (see the `gh api` lookup above).

- Project (number): `13`, owner `xtdb`
- Project (node ID): `PVT_kwDOBNKmUs4AJUwS`
- `Status` field ID: `PVTSSF_lADOBNKmUs4AJUwSzgFuIbk`
  - `🔖 Selected`: `a9f1d437`
  - `💭 Backlog`: `41a95590`
  - `🏗 In progress`: `1ef0eeb9`
  - `👀 Awaiting merge`: `34b2f44b`
  - `✅ Awaiting demo`: `3fbaabb5`
- `Stream` field ID: `PVTSSF_lADOBNKmUs4AJUwSzgTHM3Q`
  - `Long-run reliability`: `c7d77520`
  - `Operations`: `6e6dc34c`
  - `Indexing`: `b10b59ed`
  - `Multi-DB`: `549cec84`
  - `Authn/Authz`: `c4ecefe6`
  - `CDC / IVM`: `83edd538`

Issue types (org-level, set on the issue itself rather than the project board):

- `Task` — a specific piece of work: `IT_kwDOBNKmUs4A3DI3`
- `Bug` — an unexpected problem or behaviour: `IT_kwDOBNKmUs4A3DI5`
- `Feature` — a request, idea, or new functionality: `IT_kwDOBNKmUs4A3DI7`
- `Epic` — larger projects that require breaking down: `IT_kwDOBNKmUs4BnRe4`

### Delegating to the chalk `github` agent

All GitHub interaction — issue creation, comments, PR creation, project-board updates, issue-type assignment, label changes, blocked-by/sub-issue wiring — goes through the chalk `github` agent.

The chalk skill is deliberately generic; its own instructions specify that callers pass project-specific conventions (project IDs, field IDs, option IDs, issue-type IDs, labels, reviewers) verbatim in the prompt.
When delegating for anything board- or issue-type-related, paste the relevant IDs from this file directly into the agent's prompt.
Don't ask chalk to discover them; don't paraphrase.

Under the hood, chalk runs the commands below — handy to know when writing a chalk prompt or diagnosing an unexpected result:

- Add an existing issue/PR to the board: `gh project item-add 13 --owner xtdb --url <url>`
- Take an item off the board: `gh project item-archive 13 --owner xtdb --id <item-id>` — **archive, never `item-delete`**.
  Archiving keeps the card associated with the project so the tracking history survives; deleting destroys the association outright.
  This is the default for any "take this off the board" instruction, including undoing a mistaken add of your own; only delete if a human explicitly says delete.
  Gotcha: archived items are invisible to *both* `gh project item-list` and the GraphQL `ProjectV2.items` connection, so neither can distinguish "archived" from "never added" — check via the issue instead, with the `projectItems(includeArchived: true)` query above.
- Set a field on an item: `gh project item-edit --id <item-id> --project-id PVT_kwDOBNKmUs4AJUwS --field-id <field-id> --single-select-option-id <option-id>`
- Set the issue type (org-level, not exposed on plain `gh`): `gh api graphql -f query='mutation($issue:ID!,$type:ID!){ updateIssueIssueType(input:{issueId:$issue,issueTypeId:$type}){ issue { id } } }' -f issue=<issue-node-id> -f type=<type-id>`
- Add the breaking-change label: `gh issue edit N --add-label 'breaking change'` / `gh pr edit N --add-label 'breaking change'`

Re-fetch IDs if the tables above look stale:

- Project fields/options: `gh project field-list 13 --owner xtdb`, then `gh api graphql -f query='query { node(id: "<field-id>") { ... on ProjectV2SingleSelectField { options { id name } } } }'`.
- Issue types: `gh api graphql -f query='query { organization(login: "xtdb") { issueTypes(first: 20) { nodes { id name } } } }'`.
