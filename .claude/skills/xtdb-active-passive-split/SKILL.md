---
name: xtdb-active-passive-split
description: The active/passive/monitor taxonomy XTDB's concurrency is designed against — the three definitions, the mechanical test for which one you are holding, and why a lock-free monitor reads as ordinary passive state. Read this before you add or move a class, an interface, a coroutine scope, or a field that outlives a single call, and before reviewing a diff that does.
---

# The active/passive split in XTDB

Read this before you add or move a class, an interface, a coroutine scope, or a field that outlives a single call — and before you review a diff that does.

Interpret MUST, MUST NOT, SHOULD, SHOULD NOT, MAY per RFC 2119.

This skill is the *vocabulary*: three categories, and the test that tells them apart.
The names are POSA2's rather than XTDB's own, deliberately — an established name points at a literature you have already read, where a private one makes you guess.
See `references/prior-art.md` for the citations and what each one is doing.

## Active — it offers a choice among simultaneously-ready events

In Kotlin, an active object owns a `select`.

**The test: does it decide *what happens next*, or only *how to do what it was asked*?**

- **A subordinate exposes a select clause; it does not run a loop of its own.**
  An arm contributed to somebody else's `select` keeps the choice — and so the decision about what happens next — with the owner.

- **The definition is "offers the choice", not "owns a coroutine", because that is what CSP's external choice is.**
  In CSP external choice (`□`) is resolved by the environment and internal choice (`⊓`) by the process; a `select` with several clauses is external choice, occam's `ALT` directly.
  What an active object genuinely owns is the *set of events on offer*.

- **A long-running loop is not automatically active.**
  `Log.tailAll(partition, afterMsgId, processor)` suspends for as long as the tail runs, but it is driven by one source and offers no choice, so it is passive — it completes on the caller's coroutine, and the caller asked for it.

## Passive — everything it does completes on the caller's coroutine

Needing no other party.

**The test: could it finish if nobody else ever ran?**

- **Suspension is allowed.**
  `Log.appendMessage` is `suspend`, does I/O, and is passive: the caller asked for the write and gets it back on its own coroutine.

- **`coroutineScope { }` is passive despite launching.**
  It launches children *and* joins them before returning, so nothing escapes: the definition is about the boundary, not the internals.
  Internal concurrency is fine wherever it is joined before returning.

- **In Kotlin the discriminator is mechanical.**
  `coroutineScope` and `withContext` join, so passive; a stored `CoroutineScope` plus `launch` escapes, so not.
  `Log.SubscriptionListener.transitionToLeader` returns a `Deferred` running on the *listener's own* scope, which is exactly the escape — the transport joins or cancels it, rather than the transition completing under the caller.

- **The formal name for the carve-out is `runST`**: local mutation whose effects provably cannot escape is observationally pure.
  Clojure transients are the version of this that will land in this repo.

## Monitor — passive, plus state that outlives a call and callers who wait on it

Three parts: state outliving a call, callers suspending on a condition over that state, and a signal that wakes them.
A refinement *within* passive, not a third peer.

**The test: does anyone wait on it?**

- **In XTDB the mutual exclusion is often a single-writer discipline rather than a lock**, which is why a monitor can pass for ordinary passive state — there is no `synchronized` block to see.

- **A monitor's condition MUST be re-evaluable: a waiter arriving later MUST be able to reach a different verdict.**
  This is Hoare's and Brinch Hansen's rule, and breaking it is a defect class rather than a coroutine subtlety.
  A latching condition is the shape to look for.

`Watchers` is the worked example — a lock-free monitor, the absorbing-`Failed` bug as a monitor defect, and two waiter populations that were never one value.
See `references/watchers.md`.

## Three ways to misclassify

- **Don't classify by side effects.**
  Active ⟹ effectful, but passive ⇏ pure: `Log.appendMessage` is passive and does I/O, and `TermFence.admit` is passive and folds a term into a `@Volatile var`.
  Read as functional core / imperative shell, the taxonomy has autonomy substituted for purity, and the two axes are independent — so a diff cannot be classified off what it touches.

- **Don't classify by whether there is a lock.**
  A CAS plus a wait-list of continuations is a monitor; so is a `MutableStateFlow` that callers `first { }` on.

- **Passive state is not automatically a monitor — the waiters are what make it one.**
  `TermFence` holds state that outlives every call, written by one coroutine (the partition's replica-log reader) and read by another, and nobody waits on it: passive, with a single-writer discipline standing in for the lock.
  `Watchers` is that same shape plus three condition waits, which is what puts it under the monitor rule.

## The design metric: count the active objects

**Count the things that own a `select`.**
Four in `core` at `45c66b169` — `InMemoryLog`, `BlockGarbageCollector`, `TrieGarbageCollector`, `Compactor`.

- **`runBlocking` is the companion count** — ten in `Database.kt`, marking where the one-loop story meets code that does not share it.

- **Grep for `select[<{]`, not `select {`.**
  `TrieGarbageCollector` writes `select<Unit> {`, so the narrower pattern reports three where there are four; a multi-line or `whileSelect` form would slip past both, which makes any count a floor.

- **Two nearby metrics MUST NOT be used, because both read an absence of edits as a signal in work that is deliberately sequenced.**
  *File churn across sessions* measures incremental change rather than thrash — 275 edits to one file over 13 days is the well-behaved case, every one of them approved.
  *Symbol persistence against a deletion list* misreads "condemned, and therefore untouched" as "stuck" — the symbol still at 14 call sites was the next thing to go, not the laggard.

## The direction of travel

**One active object per partition, everything else passive.**
That is already visible in the merged PRs rather than being a target set here, so a diff that adds a second loop inside a partition is going against the grain and should say why.

## Not covered here

Split by what is settled: this skill carries the taxonomy, and the material below is either unsettled or deliberately excluded.

- **Unsettled, and landing in this skill when it settles**: the atomicity / lifetime / ownership questions that decide what is one value, which region holds it and which loop writes it; and the seam rules (a seam MUST NOT cut across a lifetime; a seam sits *at* the serialisation point, not across it).
- **Excluded**: "actor" (three meanings in play), rendezvous as a term, nested monitor lockout (XTDB is largely lock-free), statecharts and sequence-diagram notation.
