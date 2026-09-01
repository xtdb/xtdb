---
name: xtdb-object-boundaries
description: Where a piece of state or behaviour belongs, and which object owns it. Read this when planning a change; when deciding which type holds some state, whether two fields are one value, whether something is one object or two, or where a method goes; when adding or moving a class, interface, object or namespace; when adding a field that outlives a single call, or a coroutine scope; when splitting a type up or merging two; when threading a value through a new parameter; on a `Map<Id, State>` sitting beside the objects it identifies, or a field only set in some states; and when reviewing a diff that does any of those.
---

# Object roles and boundaries

Interpret MUST, MUST NOT, SHOULD, SHOULD NOT, MAY per RFC 2119.

**You MUST identify an object's role before you change it, and what state it owns**, and **one object holds one role**: an object that owns a `select` and also holds state others wait on is two objects that have not been separated yet.
A role is decided by the control flow an object owns, and confirmed by the state it holds.

**XTDB is a functional core with an imperative shell** (Bernhardt, 2012), with autonomy substituted for purity: the shell decides what happens next, the core only does what it was asked.
Classify by autonomy, not by side effects — `Log.appendMessage` does I/O and belongs to the core.

**Every object is an Active Object, a Passive Object or a Monitor Object** (POSA2, 2000).

## Active Object — it offers a choice among simultaneously-ready events

In Kotlin, it owns a `select`.

**Test: does it decide *what happens next*, or only *how to do what it was asked*?**

- **A subordinate exposes a select clause rather than running a loop of its own**, so the choice stays with the owner.
- **The choice makes it active, not the coroutine.**
  CSP's external choice (`□`) is resolved by the environment, internal choice (`⊓`) by the process; a `select` with several clauses is external choice, occam's `ALT` (Hoare, 1978).
- **A long-running loop is not automatically active.**
  `Log.tailAll` suspends for as long as the tail runs, is driven by one source and offers no choice — passive.

## Passive object — everything it does completes on the caller's coroutine

**Test: could it finish if nobody else ever ran?**

- **Suspension is allowed.**
  `Log.appendMessage` is `suspend` and does I/O; the caller asked for the write and gets it back on its own coroutine.
- **`coroutineScope { }` stays passive**: it joins its children before returning, so nothing escapes (`runST` — Launchbury & Peyton Jones, 1994).
- **The discriminator is mechanical.**
  `coroutineScope` and `withContext` join, so passive; a stored `CoroutineScope` plus `launch` escapes, so not.
  A method handing back a `Deferred` that runs on its own object's scope is the shape to spot.

## Monitor Object — passive state that callers wait on

State outliving a call, callers suspending on a condition over it, and a signal that wakes them.
A refinement within passive, not a third role.

**Test: does anyone wait on it?**

- **The mutual exclusion is often a single-writer discipline rather than a lock**, so there is no `synchronized` block to spot.
  `TermFence` is passive state written by one coroutine and read by another with nobody waiting — not a monitor.
- **A monitor's condition MUST be re-evaluable: a waiter arriving later MUST be able to reach a different verdict** (Hoare, 1974; Brinch Hansen, 1973).
  A latching condition is a monitor defect, and swapping the concurrency primitive does not fix it.
- **`Watchers` is the monitor to know, and the defect to recognise.**
  A `MutableStateFlow` over a sealed `Active`/`Failed` pair, one write path, several condition waits over it, and no lock in sight — which is why it read as ordinary passive state for as long as it did.
  Its `Failed` variant latches: once failed, every later waiter throws, so ingestion failing once left a queryable database unqueryable until the node restarted. A monitor defect rather than a coroutine subtlety, and it would latch identically behind a `ReentrantLock`.

## One value — fixed identity, immutable state, one swap

Clojure's epochal model (Hickey, *Are We There Yet?*, 2009): an **identity** is a stable handle, its **state** is the immutable value it holds at one moment, and time is the succession of those states.
The same boundary is Evans' **aggregate** (2003) — the unit of consistency, whose invariants are never observably violated — with an atomic swap standing in for the transaction.

**Test: name a value the identity holds in between.**
Two fields are one value exactly when you cannot.

- **The shape is a sealed hierarchy behind one reference, armed with a single `set`** — "Related state SHOULD be updated atomically" in `dev/CODING.adoc` has it.
  A reader holding a generation cannot observe a mixture of two, which is what makes it safe with no lock to point at.
- **A swap function MUST be pure**: a CAS loop may run it more than once.
- **Keep the value small** (Vernon, *Effective Aggregate Design*, 2011) — inside goes what must be consistent and nothing else.
  The placement question is *who must see this change immediately, and who can wait?*
- **Needing to swap two identities together means the boundary is in the wrong place.**
  That is `dosync` over refs: a signal, not a tool.

## Accumulating state is a transient, not an atom

A **transient** is mutable, owned by exactly one writer, and MUST NOT be published mid-flight — Clojure's transients, with `runST` (Launchbury & Peyton Jones, 1994) as the formal account.

**Test: does anyone read it while it is being built?**

- **Nobody** — mutate in place under its owner and hand it over complete.
  `PendingBlock` accumulates buffered records under the follower that owns it, and is passed on whole.
- **Somebody** — it is an atom, and every observable step MUST be a swap.
  `Watchers`' watermarks move together on each applied record precisely because callers are waiting on them.

## Region and owner

- **A region groups by lifetime; a value groups by atomicity.**
  A region is a node in the lifetime tree (structured concurrency — Sústrik, 2016; Smith, 2018) and may hold values with nothing else in common. A value has one write point.
  Conflating them buys either a torn read or a region-wide lock.
- **A reference MUST NOT outlive its owner** — ownership and RAII, which Rust makes mechanical and we do by hand.
  State read after its owner is torn down, or read before the join that made it safe, is a dangling borrow.
- **Where there is no lock, ownership is a discipline plus safe publication** (Goetz, *Java Concurrency in Practice*): one writer, and the volatile that makes its writes visible to the coroutine reading them.
