# Agents

NOTE to humans: keep this file to instructions for AI agents; if it'd be useful for humans, add it to the developer documentation instead and point the agents to it.

Interpret MUST, MUST NOT, SHOULD, SHOULD NOT, MAY, etc. per RFC 2119.

See developer documentation in `/dev` in this repo, particularly @dev/README.adoc and @dev/GIT.adoc.
`dev/doc/*.adoc` describe the high level architecture of XTDB - read those too.

We develop using 'tidy-first' methodology - endeavouring to separate 'equivalence' changes (changes which do not affect runtime behaviour, changes which increase our options) from changes that advance the behaviour of XT.
For example, even when we're working on a feature branch, we will often separate a tidying change in a separate commit and cherry-pick it onto `main`, so that the resulting PR is easier to review.

We take great inspiration from the principle of 'making illegal states unrepresentable' - prefer type systems, data structures, and APIs that prevent invalid states at compile time rather than requiring runtime validation.

* This is a Gradle project - you MUST NOT use Clojure CLI tools to run code within the project; they will not bring in the dependencies correctly.
* Modules are named `xtdb-<directory>` - e.g. `:xtdb-core`, `:modules:xtdb-kafka` - so that the Maven artifacts have the `xtdb-` prefix.
* We use conventional commits for commit messages. Common prefixes: `feat:`, `fix:`, `refactor:`, `tidy:`, `build:`, `test:`, `dev:`, `ai:`.
  * Use sub-tags/scopes in parentheses where relevant, e.g. `fix(ee):`, `feat(sql):`, `refactor(logical-plan):`.
  * Indicate breaking changes with `!`, e.g. `tidy!:`.
* If you need to create git worktrees, you MUST create them in the `.tasks` directory in the repo root without setting upstream tracking (e.g., `git worktree add .tasks/<branch-name> -b <branch-name> origin/main`). This allows developers to explicitly push to their own forks.
  * After creating a worktree, you MUST `cd` into it before performing any further operations.
  * Check whether you're in a worktree when you start up - if you are, you MUST ensure that any reads and updates are done within that worktree.
* For file operations (reading, searching, editing, writing), you SHOULD use the built-in tools (`Read`, `Edit`, `Write`, `Glob`, `Grep`).
* For REPL evaluation, use the `clj-nrepl-eval` command via Bash or the `/clojure-eval` skill (see `skills/clojure-eval/SKILL.md`).

## Definition of done

* You MUST include tests for new/changed functionality.
* You MUST run tests locally to verify they pass.

For user-visible features:
* You MUST update relevant docs in `/dev/doc/` or user-facing documentation when implementing new features or changing existing behaviour.

## Style

Use sentence-per-line in documentation files (one sentence per line) - this makes diffs cleaner and easier to review.

For comments, see the "Comments" section in @dev/README.adoc - focus on the 'why', not the 'what'.

## Running tests

- You MUST NOT run tests yourself - use sub-agents.
- For Clojure tests (testing Clojure code in `/src/test/clojure`): you SHOULD use the `repl-explorer` agent via the Task tool. This gives faster feedback and allows interactive debugging.
- For all other tests (Java, integration tests, or when REPL isn't available): you SHOULD use the `gradle-tests` agent via the Task tool.
- You MUST NOT launch multiple `gradle-tests` agents concurrently.
  Combine test namespaces into a single agent invocation instead.
  The test agent will decide what/how to invoke it.
- You SHOULD proactively run relevant tests after code changes to verify they work.
