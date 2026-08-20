---
name: xtdb-git
description: XTDB's branch-level git practices — why history stays linear, what a rebase conflict actually risks and how to resolve one safely, and the three ways a branch gets merged back to main. Read this before rebasing a branch, resolving a conflict, or merging a branch into main in this repo.
---

# Git practices in XTDB

Read this before you rebase a branch, resolve a conflict, or merge a branch back into `main`.

Interpret MUST, MUST NOT, SHOULD, SHOULD NOT, MAY per RFC 2119.

This skill covers what happens to a *branch*.
Commit messages are `chalk:commit`'s; ship/show/ask and the code-review pass are in the `== Git` section of `dev/CODING.adoc`; issues, PRs and the board are the `xtdb-github` skill's.

## Prefer a linear history

Linear history is much easier to reason about and to bisect, so **prefer rebasing a feature branch onto `main` over merging `main` into the branch**.

## A rebase conflict is where a fix gets silently reverted

**The real risk in a conflict is not the textual overlap — it's that taking the *branch's* side reverts a fix that landed on `main` after the branch forked.**
A conflict on a critical path is often two fixes competing, and which side encodes a fix is visible in the issue or PR intent, not in the diff.

This bites hardest when a stale branch is rebased across a path that has seen heavy churn, which is exactly when there are most conflicts to work through and least attention per conflict.

For each conflicting file:

1. **`git log <merge-base>..origin/main -- <file>`** to find the intervening commits.
2. **Read their PRs and issues** — that's where the intent lives.
3. **Only then choose a side.**

**After resolving, trace the fixed code path end-to-end** rather than eyeballing the marker site.
A resolution can be locally coherent and still have dropped a fix a few lines away.

## Merging a branch back into `main`

Three patterns, by the size of the change:

- **GitHub's "squash and merge"** is a reasonable default.
- **A single commit** can go straight onto `main`, once reviewed.
- **`git switch main && git merge --no-ff <branch>`** where you want the original commits visible — for `git blame`, say.
  It marks out a block of work, preserves the commits, and keeps the history linear.
