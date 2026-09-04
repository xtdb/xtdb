---
name: xtdb-github
description: XTDB-specific conventions for issues, PRs and the 2.x project board — what goes on the board, sub-issue parenting, the two tests a milestone has to pass, what to read and which edits to make when picking up a card, the cached project and field IDs, and driving all of it through chalk. Read this before opening an issue or PR, adding a card, setting a milestone or label, or picking up a card in this repo.
---

# GitHub issues, PRs and the project board in XTDB

Read this before you open an issue or PR, add a card to the board, set a milestone or a label, or pick a card up.
The chalk skills are generic — they come from the `juxt/claude-plugins` marketplace — so everything XTDB-specific they need lives here, and it is the caller's job to pass it on.

Interpret MUST, MUST NOT, SHOULD, SHOULD NOT, MAY per RFC 2119.

XTDB 2.x work is tracked on the [xtdb org "2.x" project board](https://github.com/orgs/xtdb/projects/13), with each release cut against a milestone named `2-NEXT` (which gets renamed to the release version when it ships — so the milestone *name* is stable but its number/ID changes).

The 2.x board is not the org's only project, and some work is deliberately tracked on another one.
"Not on the 2.x board" therefore does not mean "untracked".
Before adding a card on the grounds that an issue looks orphaned, you MUST check which projects already claim it:

```bash
gh api graphql -f query='query { repository(owner:"xtdb",name:"xtdb"){
  issue(number:NNNN){ projectItems(first:10, includeArchived:true){
    nodes{ isArchived project{ number title } } } } }'
```

Not all of those projects are public, so this file names none of them and you MUST NOT record their names, numbers or contents here.
You MUST treat a hit from another project as "already tracked, leave it alone".

## What goes where

When you open an issue *or* PR, you MUST work out whether it's standalone or part of a surrounding issue:

- **There's a surrounding issue** (the PR closes, advances, or is otherwise scoped by an open issue): the **issue** carries the board card, and the milestone if it passes both tests in [Milestones](#milestones) below.
  The PR does not go on the board and does not get a milestone — it inherits them through the issue.
- **It's a standalone PR** (no surrounding issue, e.g. a small fix, cleanup, or dependency bump worth noting in release notes): the **PR** goes on the board directly, and takes the milestone on the same two tests.

This mirrors how the release notes are written — one entry per issue-or-standalone-PR, never both.
Applies to docs-only and meta/repo-admin work too (1.x work is the only category that doesn't go on the 2.x board, and there's very little of that these days).

Two board fields, once the card exists:

- **`Status` is set automatically on item creation**, so you don't set it when you file — but you MUST set it when you pick the card up, per [Starting work on a card](#starting-work-on-a-card).
- **You SHOULD set `Stream` where the right category is obvious, and MUST NOT invent one.**
  Otherwise ask, or leave it blank and let a human classify it.
  A sub-issue does NOT inherit its parent's stream — the parent's category says nothing about a child whose subject is something else — so "obvious from the parent" is not obvious, and where that's the only candidate you'd have, ask.

## Sub-issues

A sub-issue's parent SHOULD be its natural surrounding issue — the prerequisite, or the closest piece of work that explains why this matters — not automatically the top-level umbrella.
Before filing one, ask which open issue creates the conditions for this work; that's the parent.

Nesting two or three deep is fine and often right: umbrella → migration → split-out.
Flattening everything onto the umbrella out of habit loses the dependency structure, which is the thing that makes "open and un-blocked" a usable queue.

## Milestones

A milestone means two different things depending on the card's state, and both readings have to keep working:

- **Open on the milestone** means *the release is waiting for this* — the open list is the "what's left before we can ship?" queue.
- **Closed on the milestone** means *this shipped in the release* — the closed list is the release-notes scope.

So a milestone set **when you create a card** MUST pass two tests, not one:

- **End-user-visible.**
  Internal refactors, module-author-facing SPI tightening, test-infra and infrastructure cleanup go on the board with an appropriate `Stream` but MUST NOT go on `2-NEXT` or any other milestone, however large they are — not at creation, and not at close either.
  End users don't read about interface tightening; putting it on the milestone clutters the release notes with internal noise.
- **Blocking the next release** — would we hold the release for this?
  If not, leave the milestone off at creation and set it when you close the card.
  By then it has definitely landed in the release, so it earns its release-notes entry without ever having claimed to be a blocker.
  Every card on the open list asserts the release is waiting, so a non-blocker dilutes exactly the signal the list exists to carry, and "is this milestone still accurate?" stops being answerable at a glance.

You MAY add the milestone to an open card mid-flight, once it's decided the work is going into this release after all.
That is the same move as setting it at close, just earlier, and it is the only reason an open card SHOULD gain one.

**A bug is not automatically a blocker**, and MUST clear the same two tests.
Reaching for the milestone by reflex is how the open list fills up with work nobody intends to hold the release for.

- **A pre-existing bug is presumptively not a blocker**: if the last release shipped with it, this one can too.
  What overrides that is a change in the bug's *situation* rather than its existence — it got worse, it became reachable on a path this release adds, or something else shipping this release makes it materially more likely to be hit.
- **Severity is the test, not the issue type.**
  A `Bug` type and a `fix:` prefix say nothing about whether the release waits for it.
  Silent data loss or corruption usually does qualify, as does wedging a node or a database with no operator route out; a wrong error message on a narrow edge case usually doesn't.
- **Wrong observability can be load-bearing for something else on the milestone.**
  A metric that reads healthy when it knows nothing is ordinarily its own card, and becomes a blocker when a change already in the release makes that metric the signal an operator is told to rely on.

**Where you can't tell, you MUST ask rather than default.**
Defaulting *on* is the more expensive mistake: an absent milestone is caught when the card closes, whereas a wrong one quietly misrepresents the release scope until someone audits it.

The open milestone is always named `2-NEXT`.
You MUST look up its current REST number by name+state rather than caching it:

```bash
gh api '/repos/xtdb/xtdb/milestones?state=open' --jq '.[] | select(.title=="2-NEXT") | .number'
```

Set it on an issue or PR with `gh issue edit N --milestone 2-NEXT` / `gh pr edit N --milestone 2-NEXT`, and take it off with `gh issue edit N --remove-milestone` / `gh pr edit N --remove-milestone`.
Removing a milestone leaves the board card and its fields untouched, so it is the right move for a card that turned out not to be blocking — it drops out of the release scope and stays tracked.

## Labels

We don't make heavy use of labels, but two conventions matter for release notes:

- **`breaking change`** (note: space, not hyphen): you MUST apply this to any issue/PR that's a user-impacting breaking change.
  Same scope as the commit-message `!` rule documented in `AGENTS.md` — SQL syntax, pgwire protocol, config YAML, public Java/Kotlin API.
  Internal refactors don't count.
- **Component labels**: long-tailed set of area tags (`sql`, `pgwire`, `kafka`, `compactor`, `indexing`, `logical-plan`, `expression engine`, `xtql`, `docker`, `docs`, `dev-experience`, `performance`, etc.).
  You MAY apply one where a single area is obviously the subject.
  You MUST fetch the current list with `gh api '/repos/xtdb/xtdb/labels?per_page=100' --jq '.[].name'` rather than guessing at a name.

## Starting work on a card

### Read the neighbourhood first

You MUST read the issue, its recent comments, and its one-hop neighbourhood — parent, sub-issues, blocked-by, blocking — before you touch any code.
An issue read on its own can look ready while its blocker is still open, or restate a decision its parent has already settled.

With chalk in use, its `github` agent fetches the neighbourhood in a single GraphQL call.
Otherwise, `gh issue view N --comments` plus:

```bash
gh api graphql -f query='
  query($number: Int!) {
    repository(owner: "xtdb", name: "xtdb") {
      issue(number: $number) {
        parent { number title state }
        subIssues(first: 20) { nodes { number title state } }
        blockedBy(first: 20) { nodes { number title state } }
        blocking(first: 20) { nodes { number title state } }
      }
    }
  }' -F number=N
```

Where the change is non-trivial and the *why* or the *why now* isn't obvious from the issue or its neighbours, you MUST ask before starting.
A *why now* MUST be traceable to something the user said, a commit, or a file you can name.
`chalk:issue`, `chalk:commit` and `chalk:pr` ask the same question once the artefact is drafted, which is after the code is written.

### Say so on the card

When you pick up an issue or PR, you MUST say so on the card *before* you start working on it.
Two edits, and they're the whole of it:

- **Assign it to yourself.**
  The assignee is whoever is currently responsible for moving the item forward (see [The assignee owns progression](#the-assignee-owns-progression) below), so `@me` iff you're *about to work on it*.
  An issue or PR you're filing but not starting immediately stays unassigned.
  `gh issue edit N --add-assignee @me` / `gh pr edit N --add-assignee @me`.
- **Set board `Status` to `🏗 In progress`.**
  `gh project item-edit --id <item-id> --project-id PVT_kwDOBNKmUs4AJUwS --field-id PVTSSF_lADOBNKmUs4AJUwSzgFuIbk --single-select-option-id 1ef0eeb9`
  Get `<item-id>` from `gh project item-list 13 --owner xtdb --format json`, matching on the issue number — it's the *project item's* id, not the issue's.

The board is where everybody else sees who is on what, and for anyone outside your session it's the only place they can see it.
An unassigned card sitting in `🔖 Selected` while somebody is halfway through the work is how two people start the same thing.

The chalk skills do both of these for you — see [Using chalk](#using-chalk).

## Cached IDs

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

## Using chalk

**Where the chalk plugin is available, use it** — `chalk:issue` for issues, `chalk:pr` for pull requests, `chalk:commit` for commit messages.
It carries the voice and structure those artefacts are held to, and its `github` agent does the board, assignee and issue-type mechanics for you.

**Where it isn't available, the conventions still hold.**
You MUST write the artefacts to the conventions in this file, and make the board and assignee edits with plain `gh`.
Chalk is the convenient route to the conventions rather than the source of them, so "chalk wasn't loaded" is not a reason for an unassigned card, a card left in `🔖 Selected`, or an issue body with no *why*.

With chalk in use, two rules:

- **Every GitHub interaction it covers MUST go through its `github` agent rather than raw `gh`** — issue creation, comments, PR creation, project-board updates, issue-type assignment, label changes, blocked-by/sub-issue wiring.
  That is what stops the voice guidance in the calling skill being bypassed.
- **You MUST paste the IDs from this file into the agent's prompt verbatim** for anything board- or issue-type-related — project IDs, field IDs, option IDs, issue-type IDs, labels, reviewers.
  Chalk is deliberately generic and its own instructions require the caller to supply these, so you MUST NOT ask it to discover them, and MUST NOT paraphrase them.

Under the hood, chalk runs the commands below:

- Add an existing issue/PR to the board: `gh project item-add 13 --owner xtdb --url <url>`
- Take an item off the board: `gh project item-archive 13 --owner xtdb --id <item-id>` — **archive, and you MUST NOT `item-delete`** unless a human explicitly says delete.
  Archiving keeps the card associated with the project so the tracking history survives; deleting destroys the association outright.
  Archiving is the default for any "take this off the board" instruction, including undoing a mistaken add of your own.
  Gotcha: archived items are invisible to *both* `gh project item-list` and the GraphQL `ProjectV2.items` connection, so neither can distinguish "archived" from "never added" — check via the issue instead, with the `projectItems(includeArchived: true)` query above.
- Set a field on an item: `gh project item-edit --id <item-id> --project-id PVT_kwDOBNKmUs4AJUwS --field-id <field-id> --single-select-option-id <option-id>`
- Set the issue type (org-level, not exposed on plain `gh`): `gh api graphql -f query='mutation($issue:ID!,$type:ID!){ updateIssueIssueType(input:{issueId:$issue,issueTypeId:$type}){ issue { id } } }' -f issue=<issue-node-id> -f type=<type-id>`
- Add the breaking-change label: `gh issue edit N --add-label 'breaking change'` / `gh pr edit N --add-label 'breaking change'`

Re-fetch IDs where the tables above look stale:

- Project fields/options: `gh project field-list 13 --owner xtdb`, then `gh api graphql -f query='query { node(id: "<field-id>") { ... on ProjectV2SingleSelectField { options { id name } } } }'`.
- Issue types: `gh api graphql -f query='query { organization(login: "xtdb") { issueTypes(first: 20) { nodes { id name } } } }'`.

## The assignee owns progression

**The assignee is whoever is currently responsible for moving the item forward, end-to-end.**
That means chasing whoever needs chasing, unblocking what needs unblocking, replying to whoever needs replying to, and working out what needs doing.
It doesn't mean doing all of it yourself — delegate where that helps — but the coordination is yours.

- **You MUST keep the card description accurate as the work changes.**
  Journaling comments are welcome on top, but the description is what lets a reader understand the current state without reducing over the comment history.
  Updating it is `chalk:issue`'s job.
- **You MUST keep `Status` correct.**
  Awaiting review means the reviewer is actually on the PR; needing a re-review means pressing the re-review button, so they're notified.
- **On a PR, the assignee holds the lock on the branch.**

**You MUST fix the assignee whenever it's wrong** — if it's you and it shouldn't be, or it's not you and it should be, change it, including to nobody.
That's the more common case, and it costs nothing.

## Branches live on your fork

You SHOULD work from a fork of `xtdb/xtdb` rather than pushing branches to the main repo, so branch cleanup is yours rather than everyone's.
