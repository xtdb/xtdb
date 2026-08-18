---
name: xtdb-testing
description: XTDB-specific rules and mechanics for running tests — delegation, the mid-run edit freeze, test tasks and filters, iteration counts, simulation-test coverage, diagnosing failures, and regenerating arrow-edn golden fixtures. Read this before running or delegating any test run in this repo.
---

# Running tests in XTDB

Read this before you run a test, and before you delegate a test run to a sub-agent.
The `gradle-tests` agent is generic — it comes from the `xtdb/claude-plugins` marketplace — so everything XTDB-specific it needs lives here, and it is the caller's job to pass it on.

Interpret MUST, MUST NOT, SHOULD, SHOULD NOT, MAY per RFC 2119.

## The rules you MUST NOT get wrong

1. You MUST NOT run tests yourself — delegate to the `gradle-tests` agent.
2. You MUST NOT edit files the build compiles while a run in that worktree is still compiling them — see [The build phase is what is frozen](#the-build-phase-is-what-is-frozen).
3. You MUST tell `gradle-tests`, in every invocation, not to modify any source file.
4. A test that fails after your change is a test *you* broke — see [When a test fails](#when-a-test-fails).

The rest of this document is the mechanics behind those four.

## Delegating to `gradle-tests`

Use the `gradle-tests` agent via the Task tool for *all* test runs, Clojure included.

- You MUST NOT use `repl-explorer` to run tests.
  Its own workflow tells it to "modify code and reload" when a test fails, so it edits source on your behalf and reports green.
  The `/clojure-eval` skill and `clj-nrepl-eval` remain the right tools for *exploratory* evaluation — inspecting state, trying an expression, reproducing a bug.
  They are not the tool for "run the tests and tell me whether they pass".
- Every `gradle-tests` prompt MUST state explicitly that the agent is to run and report only, and MUST NOT create, edit or delete any file.
  Its definition already says so and it holds no `Edit`/`Write` tool, but it holds `Bash`, and the definition is maintained outside this repo — say it anyway.
- You MUST NOT launch more than one `gradle-tests` agent concurrently.
  Combine every namespace you want covered into a single invocation and let the agent choose how to run them; Gradle parallelises internally.
- You SHOULD run the relevant tests proactively after a code change rather than waiting to be asked.

## The build phase is what is frozen

The freeze is scoped to the worktree the run is compiling from, and within it to **the files that run compiles** — Kotlin, Java and Clojure sources, build scripts, and anything on the test resource path.
Recompiling under a running build produces **bogus cross-language type errors and cascading failures** that look exactly like real breakage, and chasing them costs far more than waiting did.
If you have already edited mid-compile, discard that run's results entirely and re-run once the tree is stable — do not try to reason about which failures were real.

Compilation is the part a concurrent edit corrupts, so when you already know you want to keep working, run the build phase yourself first and delegate only the execution:

1. `./gradlew :testClasses`, or `:xtdb-core:testClasses` for a module run.
   That is the entire compile phase behind the root `test` task: every module's `compileKotlin` and `jar`, plus `compileTestClojure` and `compileTestFixturesClojure`, which AOT-compile the Clojure test and fixture namespaces into `build/clojure/`.
   This is a compile, not a test run, so rule 1 does not apply and you MAY run it yourself — but the freeze does apply while it is in flight.
2. Delegate the test run as normal.
   Its compile tasks are up-to-date and clear in a few seconds on a warm daemon, and nothing is compiled after that.
3. Edit from that point on.

Unsplit, the freeze covers however long the run takes to build — minutes, after a Kotlin change or a cold `build/`.
Split, it is the few seconds at the top of the test invocation.

Two things the split does not buy:

- **The report describes the tree you compiled, not the tree you now have.**
  Anything edited after step 1 is untested until the next run, whatever the report says, so you MUST NOT report green for a file you edited during the run that produced it.
  The split lets you get on with the *next* increment while a run confirms the last one; it does not let you fix a failure and claim the same run vindicates the fix.
- **A module test run keeps that module's `src/main/clojure` live on the classpath.**
  A Clojure source set that isn't AOT-compiled has its source directory as its `sourceSet.output`, so `:xtdb-core:test` loads `core/src/main/clojure` from source as each namespace is first required.
  An edit therefore lands in the running JVM and the run executes a mixture of old and new — silently, with no compile error to give it away.
  Root `:test` is not exposed to this, because module Clojure reaches it through the jars built in step 1 and its own test and fixture namespaces are AOT-compiled.
  So `.clj` edits stay frozen for the duration of a module run, however you split it.

Everything the build never reads is outside the freeze, and you SHOULD carry on with it rather than idling: `.allium` specs, `docs/`, `dev/`, `README`s, `.claude/`.
A comment-only change still counts as a source edit — the compiler doesn't know it was only a comment.

Runs in other worktrees do not concern you, and you MUST NOT check for them or wait on them.
Each worktree has its own `build/` and `.gradle/`, and Gradle takes cross-process locks over the shared `~/.gradle` caches, so a build elsewhere on the machine cannot corrupt yours.
Those locks block rather than fail, so a run that stalls early — typically reporting that it is waiting to acquire a lock — is that mechanism working; wait it out rather than killing the run.

## Test tasks

- `./gradlew test` — unit tests.
  Excludes the `integration`, `property`, `jdbc`, `timescale`, `s3`, `minio`, `slt`, `docker`, `azure` and `google-cloud` tags.
- `./gradlew integration-test` — integration tests, longer running.
- `./gradlew property-test` — property-based and simulation tests.
- `./gradlew kafka-test` — tests needing Kafka; requires `docker-compose up`.
- `./gradlew nightly-test` — the cloud-object-store tags (`s3`, `google-cloud`, `azure`).

## Module addressing

Modules are named `xtdb-<directory>`, matching the Maven artifact prefix.

- Top-level: `:xtdb-core:test`, `:xtdb-api:test`.
- Under `modules/`: `:modules:xtdb-kafka:test`, `:modules:xtdb-aws:test`.
- Bare `:test` is the root module, where most Clojure tests live (`src/test/clojure`).

## Test filtering

- Clojure namespaces use underscores in `--tests` patterns, not dashes — `xtdb.api_test`, not `xtdb.api-test`.
- `./gradlew :test --tests 'xtdb.api_test*'` — a namespace.
- `./gradlew :test --tests '*expression*'` — a wildcard.
- `./gradlew :test --tests '**can-manually-specify-system-time-47**'` — one test.
- Re-running the *same* `--tests` invocation is cached as UP-TO-DATE and does nothing.
  Add `--rerun-tasks` whenever the point of the run is to re-execute — verifying an intermittent failure, or checking a regenerated fixture.

## Iteration counts

There are **two** independent iteration knobs on `property-test`, and the obvious one only drives half the suite.

| Property | System property | Read by |
| --- | --- | --- |
| `-Piterations=N` | `xtdb.property-test-iterations` | `tu/property-test-iterations` in `src/testFixtures/clojure/xtdb/test_util.clj` — the `:num-tests` of the Clojure test.check properties |
| `-PsimulationIterations=N` | `xtdb.simulation-test-iterations` | `DEFAULT_ITERATIONS` in `core/src/test/kotlin/xtdb/SimulationTestBase.kt` — the invocation count of each `@RepeatableSimulationTest` |

Both default to 100.

So `./gradlew property-test --tests '*SimulationTest*' -Piterations=500` runs **100** iterations per simulation method, not 500 — you get a second fresh-seed run of the same length, not a longer one.
This has nothing to do with `--tests`; `-Piterations` simply does not reach the Kotlin simulations.
To lengthen a simulation run, pass `-PsimulationIterations=N`.

You MUST read the actual iteration count out of the test output before claiming a higher-iteration run was performed.

Two per-method overrides beat both properties:

- `@RepeatableSimulationTest(iterations = N)` fixes the count for that method.
- `@WithSeed(seed = N)` pins the seed and runs exactly one iteration — this is how you reproduce a reported failure, since `SeedExtension` logs `Test failed with seed: …` and reraises as `AssertionError("Test threw an exception (seed=…)")`.

## Simulation tests are invisible to `./gradlew test`

The seeded simulation classes carry `@Tag("property")` at class level, and `./gradlew test` excludes that tag.
A change to **indexing, compaction or GC** that breaks them therefore looks green locally and only fails in CI's property job.

If you have touched those subsystems you MUST also run `./gradlew property-test`.

The classes concerned:

- `core/src/test/kotlin/xtdb/NodeSimulationTest.kt`
- `core/src/test/kotlin/xtdb/cache/CacheSimulationTest.kt`
- `core/src/test/kotlin/xtdb/compactor/CompactorSimulationTest.kt`
- `core/src/test/kotlin/xtdb/indexer/LeaderDriverSimTest.kt`
- `core/src/test/kotlin/xtdb/indexer/LogProcessorSimTest.kt`
- `modules/postgres-source/src/test/kotlin/xtdb/postgres/PostgresSourceSimulationTest.kt`
- `modules/postgres-source/src/test/kotlin/xtdb/postgres/PostgresSourceTypesPropertyTest.kt`

## Typical durations

Budget for compilation and reporting overhead as well as the tests themselves.

- Single namespace: 30–60s.
- Module test suite: 2–5 min.
- Full project suite: 10+ min.
- Integration tests: 5–15 min, I/O bound.
- Property tests: varies with iteration count.

## What a run does not prove

- **`:compileTestClojure` is the cheap Clojure check; `:compileClojure` is not.**
  The root project has no `src/main/clojure` at all, so `:compileClojure` has nothing to compile and goes green without loading a line.
  `:compileTestClojure` and `:compileTestFixturesClojure` AOT-compile every test and fixture namespace — no `--tests` filter applies — which loads everything those namespaces require, module Clojure included, so a namespace that no longer loads fails there.
  Module `main` Clojure is not AOT-compiled and nothing checks it in its own right; a test namespace requiring it is what catches it.
- **A green run does not prove absence of reflection.**
  The Gradle build never sets `*warn-on-reflection*` — only the dev REPL does, via `src/dev/clojure/user.clj`.
  If reflection is the question, answer it in the REPL; don't add type hints speculatively on a reviewer's say-so.

## When a test fails

**All tests pass on `main`. There are no pre-existing failures.**
If a test fails after your change, you broke it.
Investigate your own diff, find the bug, fix it.

- You MUST NOT speculate that a failure might be pre-existing.
- You MUST NOT stash your changes or check out `main` to "verify" that theory.
- You MUST NOT disable, skip or loosen an assertion to get to green.

The one carve-out, and it is deliberately narrow:

- You MAY check `gh issue list --label flaky` **if you genuinely believe the failure is a known flake** — the failing test is in a subsystem your change does not touch, or the failure message is about timing, ordering or resource contention rather than about behaviour.
- An open issue labelled `flaky` that matches the failure you are looking at is the **only** acceptable evidence.
  "It passed on the retry", "it looks racy", and "this test is known to be slow" are not.
- Absent a matching issue, the failure is yours. Fix it.
- If the failure is a genuine flake with no issue yet, open one and label it `flaky` rather than passing the problem on silently.

## Regenerating arrow-edn golden fixtures

Several namespaces assert a live run against committed `.arrow.edn` fixtures under `src/test/resources/xtdb/` — `xtdb.log-test`, `xtdb.indexer-test`, `xtdb.indexer.live-index-test`, `xtdb.indexer.live-table-test`, `xtdb.database-test`, `xtdb.metadata-test`, `xtdb.compactor-test`.
`xtdb.check-pbuf` reads the same toggle for its `.binpb.edn` fixtures, so anything below applies to those too.

To regenerate after an intended serialization change:

1. Uncomment the `#_aet/wrap-regen` line in that namespace's `use-fixtures` — it binds `xtdb.arrow-edn-test/*regen?*` true for that namespace only.
   Prefer this over flipping the `*regen?*` default: a blanket regen rewrites every fixture the run touches and masks unintended drift.
2. Run the namespace with `--rerun-tasks` — Gradle caches a repeated `--tests` invocation as UP-TO-DATE and does nothing.
3. Copy the regenerated files back into `src/test/resources/`, then re-comment the toggle and re-run with `--rerun-tasks` to verify green.

Gotchas, all of which have cost real time:

- **The output does not land in `src/`.**
  Expected paths resolve through `io/resource`, which under the Gradle `test` task is `build/resources/test/xtdb/…`.
  `git status src/` after a regen run shows nothing; you MUST copy the files back yourself.
- **A regen run tells you nothing about correctness.**
  `check-arrow-edn-dir` writes the expected file from the actual one and *then* compares, so it is trivially green; `maybe-write-arrow-edn!` (the `xtdb.log-test` shape) reads the old fixture before writing the new one, so that assertion fails exactly once by design and the new bytes land anyway.
  Either way the only meaningful verification is a re-run with the toggle off.
- **Do NOT `git rm` a fixture to force a regen.**
  `io/resource` returns nil for a missing resource and the write path breaks — regen only works against a fixture that already exists.
- **Comparison walks the expected tree**, so a file the run produces that the fixture lacks is silently unchecked.
  A genuinely new fixture file has to arrive via the regen path.
- **Both toggles are tagged `<<no-commit>>`** and the `.githooks/pre-commit` hook aborts a commit whose staged diff contains that marker.
  If the hook fires, you left a toggle on — don't `--no-verify` past it.
- **Never pin `xt$txs` in a golden file.**
  A tx-id is a message id derived from the log offset, so the same sequence of transactions yields different tx-ids from run to run.
