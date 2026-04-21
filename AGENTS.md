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
  * Indicate breaking changes with `!`, e.g. `tidy!:`.
    `!` only applies to **user-facing** API changes (SQL syntax, pgwire protocol, config YAML, public Java/Kotlin API).
    Internal refactors (e.g. changing internal types like `TransactionResult`, `Watchers`) are not breaking even if the types are technically `public` in Kotlin.
  * You MUST use the commit skill for writing commit messages.
* For file operations (reading, searching, editing, writing), you SHOULD use the built-in tools (`Read`, `Edit`, `Write`, `Glob`, `Grep`).
* For REPL evaluation, use the `clj-nrepl-eval` command via Bash or the `/clojure-eval` skill (see `skills/clojure-eval/SKILL.md`).

## Definition of Done (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

* You MUST include tests for new/changed functionality.
* You MUST run tests locally to verify they pass.
* You MUST update the Allium specs if you've made changes in those areas.
* The full test suite MUST pass (`./gradlew test`).
  If you've affected any integration tests (e.g. Kafka, remote storage), you MUST also run `./gradlew integration-test`.
  CI will run integration tests regardless, but `./gradlew test` is the minimum.
  You can assume that all tests are passing on `main`.
* There MUST NOT be any reflection or boxed math warnings.
* Verify: all changes committed AND pushed
  You MUST use the commit skill to create commit messages.
* Hand off: provide context for next session

For user-visible features:
* You MUST update relevant docs in `/dev/doc/` or user-facing documentation when implementing new features or changing existing behaviour.

## Style

You MUST use sentence-per-line in documentation files and commit messages - this makes diffs cleaner and easier to review.
You MUST use the commit skill to create commit messages.

For comments, see the "Comments" section in @dev/README.adoc - focus on the 'why', not the 'what'.

For errors, see the "Errors" section in @dev/README.adoc — use `xtdb.error`, not raw Java exceptions.

## Running tests

- You MUST NOT run tests yourself - use sub-agents.
- For Clojure tests (testing Clojure code in `/src/test/clojure`): you SHOULD use the `repl-explorer` agent via the Task tool. This gives faster feedback and allows interactive debugging.
- For all other tests (Java, integration tests, or when REPL isn't available): you SHOULD use the `gradle-tests` agent via the Task tool.
- You MUST NOT launch multiple `gradle-tests` agents concurrently.
  Combine test namespaces into a single agent invocation instead.
  The test agent will decide what/how to invoke it.
- You SHOULD proactively run relevant tests after code changes to verify they work.

## GitHub project board, milestones, labels

XTDB 2.x work is tracked on the https://github.com/orgs/xtdb/projects/13[xtdb org "2.x" project board], with each release cut against a milestone named `2-NEXT` (which gets renamed to the release version when it ships — so the milestone *name* is stable but its number/ID changes).

### What goes where

When you open an issue *or* PR, work out whether it's standalone or part of a surrounding issue, then:

- **There's a surrounding issue** (the PR closes, advances, or is otherwise scoped by an open issue): the **issue** carries the board card and the milestone.
  The PR does not go on the board and does not get a milestone — it inherits them through the issue.
- **It's a standalone PR** (no surrounding issue, e.g. a small fix, cleanup, or dependency bump worth noting in release notes): the **PR** goes on the board and on the milestone directly.

This mirrors how the release notes are written — one entry per issue-or-standalone-PR, never both.
Applies to docs-only and meta/repo-admin work too (1.x work is the only category that doesn't go on the 2.x board, and there's very little of that these days).

Board `Status` is set automatically on item creation — you don't need to manage it.
`Stream` is preferable to have set, but don't make one up: set it when the right category is obvious, otherwise leave it blank and let a human classify it.

### Milestones

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

Sub-issue and blocked-by relationships are not exposed on the plain `gh` CLI, but the chalk `github` agent already knows how to manage them via `gh api graphql` — delegate to it rather than re-deriving the mutations.

### Common commands

The project-board and issue-type pieces are xtdb-specific and not covered by the chalk agent:

- Add an existing issue/PR to the board: `gh project item-add 13 --owner xtdb --url <url>`
- Set a field on an item: `gh project item-edit --id <item-id> --project-id PVT_kwDOBNKmUs4AJUwS --field-id <field-id> --single-select-option-id <option-id>`
- Set the issue type: `gh api graphql -f query='mutation($issue:ID!,$type:ID!){ updateIssueIssueType(input:{issueId:$issue,issueTypeId:$type}){ issue { id } } }' -f issue=<issue-node-id> -f type=<type-id>`
- Add the breaking-change label: `gh issue edit N --add-label 'breaking change'` / `gh pr edit N --add-label 'breaking change'`
- Re-fetch field/option IDs if this list looks stale: `gh project field-list 13 --owner xtdb`, then `gh api graphql -f query='query { node(id: "<field-id>") { ... on ProjectV2SingleSelectField { options { id name } } } }'`.
- Re-fetch issue type IDs: `gh api graphql -f query='query { organization(login: "xtdb") { issueTypes(first: 20) { nodes { id name } } } }'`.
