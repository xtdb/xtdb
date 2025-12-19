# Agents

* This is a Gradle project - do not use Clojure CLI tools to run code within the project, they will not bring in the dependencies correctly.
* Modules are named `xtdb-<directory>` - e.g. `:xtdb-core`, `:modules:xtdb-kafka` - so that the Maven artifacts have the `xtdb-` prefix.

## 1. General workflow

When beginning any session:

1. Choose an issue or task ID from the external backlog.
2. Start work using the `--workon <task-id>` workflow described in Section 4.

---

## 2. Coding principles

* **Clarity over cleverness**: prefer simple, readable solutions.
* **Minimal, focused changes**: make the smallest change that solves the problem well.
* **Consistency with existing patterns**: match naming, structure, and design already used in the repo.
* **Preserve existing behavior unless explicitly changing it**.
* **Respect architecture**: extend existing abstractions rather than invent new ones.
* **Error handling**: follow established error-handling conventions; avoid silent failures.
* **Dependencies**: avoid adding new libraries unless absolutely necessary and justified.
* We develop using 'tidy-first' methodology - endeavouring to separate 'equivalence' changes (changes which do not affect runtime behaviour, changes which increase our options) from changes that advance the behaviour of XT.
* For example, even when we're working on a feature branch, we will often separate a tidying change in a separate commit and cherry-pick it onto `main`, so that the resulting PR is easier to review.

---

## 3. Testing principles

We value **meaningful, behavior-focused tests**, not raw coverage numbers.

* Test key paths, edge cases, and important error conditions.
* Avoid breaking real behavior just to satisfy tests.
* Prefer realistic tests that reflect actual usage, not implementation details.
* Follow established testing conventions in the repository.
* When adding new features, create or update tests in a way that protects important logic.

---

## 4. Task system and `llm-tasks/`

Task files are maintained primarily for LLM continuity across sessions.
They capture **current understanding**, **current work**, and **useful discoveries**, not historical logs.

### 4.1 Location and naming

Task documents live in: `./llm-tasks/<task-id>.md`

* `<task-id>` generally corresponds to a ticket or issue ID (e.g. `PROJ-123`, `GH-42`).
* The **issue tracker is the single source of truth for task status**.
* Task docs do **not** track done/active/abandoned state.

### 4.2 Template

`./llm-tasks/task-template.md` defines the structure for new tasks.
When invoking `--workon <task-id>`:

1. If the task file exists, load it.
2. If not, copy `task-template.md` to `./llm-tasks/<task-id>.md` and fill in any placeholders.

Task documents generally include sections such as:

* **Task Summary** – A concise restatement of what the task is about.
* **Current Understanding** – The current interpretation of the problem and its constraints.
* **What We Need to Do** – A list of the current actionable items or subtasks.
* **Important Learnings** – Discoveries, insights, or clarifications gained during development.
* **Important Files / Components** – Key parts of the codebase relevant to the task.
* **Reasoning Notes** – Explanations where needed (e.g. why earlier understanding was rewritten or discarded).

> These files are meant to be readable by LLMs and may be verbose.
> Human readability is secondary but should still be coherent.

### 4.3 How task documents evolve

Task docs represent the **current best understanding**, not a historical record.
They **can and should be rewritten** when new insights make old content obsolete.

However:

* When rewriting or replacing existing sections, the task document should include a brief note in **Reasoning Notes** explaining *why* the rewrite occurred.
* The goal is clarity, not log-keeping.
* Outdated or incorrect sections should be removed or replaced to avoid misleading future sessions.

### 4.4 `--workon <task-id>` command behavior

When `--workon <task-id>` is used:

* Load the corresponding task file.
* Keep the task file updated with:

  * Newly discovered constraints or clarifications.
  * Updates to the list of things that need to be done.
  * New learnings about the codebase.
  * Notes explaining meaningful rewrites.

During active work:

* The agent should update the **specific, relevant sections** of the task file.
* Broad rewrites are allowed **only when clearly beneficial**, and must include a brief explanation.

---

## 5. Editing rules for task files

To keep task documents stable, coherent, and useful for LLMs:

1. **Preserve the template’s section structure**

   * Do not remove or rename major sections unless explicitly instructed.

2. **Rewrite freely when needed**

   * If new information invalidates earlier content, rewrite those portions.
   * Major rewrites must include a brief explanation in **Reasoning Notes**.

3. **Focus on current truth**

   * Do not maintain historical logs or timestamps.
   * Only keep what is currently correct, useful, and relevant.

4. **Keep content concise where possible**

   * Avoid overly detailed logs, stack traces, or full code listings.
   * Summaries and short explanations are preferred.

5. **Never include secrets or sensitive data**

   * Replace any sensitive material with placeholders such as `[REDACTED]`.

6. **Assume human review**

   * All task doc changes are subject to human review before merge.

---

## 6. LLM editing rules for code and tests

When modifying code or tests:

* Make targeted, minimal changes.
* Preserve formatting and style.
* Avoid refactoring unless explicitly asked.
* Do not modify large unrelated regions of code.
* Ensure updates align with the task document’s current understanding.

### Tests

* Prioritize meaningful coverage: critical branches, behaviors, and edge cases.
* Avoid brittle tests tied to internal implementation details.
* Do not degrade real behavior to satisfy tests.
* Fix incorrect tests where appropriate.

---

## 7. Security, privacy, and sensitive data

* Never commit or write secrets, tokens, passwords, or user data.
* When in doubt, anonymize.
* Any suspected sensitive data must be replaced with `[REDACTED]`.

---

## 8. Sources of truth

* **Architecture & key concepts:** See developer documentation in `/dev` in this repo:
  * Particularly `dev/README.adoc` and `dev/GIT.adoc`.
  * `dev/doc/*.adoc` describe the high level architecture of XTDB - read those too.
* **Current understanding of an individual task:** `./llm-tasks/<task-id>.md`
* **Task status and lifecycle:** external issue tracker
* **Technical decisions made during development:** included in the task doc under *Reasoning Notes*
