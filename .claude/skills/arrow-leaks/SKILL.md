---
name: arrow-leaks
description: Track down an Arrow "Memory was leaked" error in XTDB — enabling the debug allocator to get the allocation site, the failure-path cleanup gaps that cause most of them, and why property-test shrinking misattributes them. Read this when a test or a node reports leaked Arrow memory.
---

# Debugging Arrow memory leaks

Read this when something reports `Memory was leaked`.

## The error reports the wrong stack

**An Arrow "Memory was leaked" error carries the *close-time detection* stack, not the allocation site.**
To get the allocation site, enable Arrow's debug allocator: the leak error then gains an `event log for Allocator(...)` section with `ALLOCATE` stack traces.

**`-D` on the command line and `JAVA_TOOL_OPTIONS` will not work.**
`build.gradle.kts` pins `-Darrow.memory.debug.allocator=false` in `defaultJvmArgs`, which every `Test`, `JavaExec` and `clojureRepl` task inherits, and that overrides anything passed in.

Flip that entry to `=true` to hunt, and **flip it back before committing** — the debug allocator does per-allocation bookkeeping.

## Most XTDB leaks are failure-path cleanup gaps

A buffer is opened and then orphaned because an exception unwinds past its close.

**Look for `openSlice` and other `open*` calls whose result is handed off without a `.use` or `closeOnCatch`, on a path that can now throw.**
`xtdb.util/closeOnCatch` closes on exception only, which is what you want where the success path's consumer already owns the buffer.

## Property-test shrinking misattributes leaks

**The shrinker assumes a determinism that leaks don't have, so its `:smallest` case can point at the wrong operation.**

Bisect with the property test across commits at a high `-Piterations`, then confirm a single-op repro with a plain `deftest` under the debug allocator.

## The allocator name is the subsystem

The leaking allocator identifies where to look — `leader-log-processor` is tx indexing, not the query allocator.
