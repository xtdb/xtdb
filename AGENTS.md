# Agents

NOTE to humans: keep this file to instructions for AI agents; if it'd be useful for humans, add it to the developer documentation instead and point the agents to it.

See developer documentation in `/dev` in this repo, particularly @dev/README.adoc and @dev/GIT.adoc.
`dev/doc/*.adoc` describe the high level architecture of XTDB - read those too.

We develop using 'tidy-first' methodology - endeavouring to separate 'equivalence' changes (changes which do not affect runtime behaviour, changes which increase our options) from changes that advance the behaviour of XT.
For example, even when we're working on a feature branch, we will often separate a tidying change in a separate commit and cherry-pick it onto `main`, so that the resulting PR is easier to review.

* This is a Gradle project - do not use Clojure CLI tools to run code within the project, they will not bring in the dependencies correctly.
* Modules are named `xtdb-<directory>` - e.g. `:xtdb-core`, `:modules:xtdb-kafka` - so that the Maven artifacts have the `xtdb-` prefix.
* If you need to create git worktrees, create them in the `.tasks` directory in the repo root.
* When tests fail, read the test report at `build/reports/tests/test/index.html` instead of re-running tests with different flags to get error details.
* For file operations (reading, searching, editing, writing), use the built-in tools (`Read`, `Edit`, `Write`, `Glob`, `Grep`).
* For REPL evaluation, use the `clj-nrepl-eval` command via Bash or the `/clojure-eval` skill (see `skills/clojure-eval/SKILL.md`).
