---
name: jstack
description: Capture and analyse a Java thread dump from XTDB — deadlocks, blocked threads, lock contention, exhausted pools. Read this when a test run or a node hangs, a Gradle worker stops making progress, or a thread dump needs interpreting. Covers capturing off a running GradleWorkerMain and delegating the parse so the dump stays out of the main context.
---

# Analysing a Java thread dump

Read this when something has hung — a test run stops making progress, a node stops responding, a Gradle worker sits idle — or when a thread dump needs interpreting.

## Capture

Skip to *Analyse* if a dump file already exists.

To take one off a running Gradle worker:

```bash
jps -l | grep GradleWorkerMain
jstack <PID> > /tmp/jstack-<PID>.txt
```

Where there's more than one worker, ask which PID to analyse rather than guessing — or take all of them if the hang could be cross-worker.

## Analyse in a sub-agent, not here

**A thread dump is tens of thousands of tokens of stack traces, and almost none of it is the answer.**
Delegate the parse and keep the dump out of the main context — read the sub-agent's findings, not the file.

- **Use a cheap model.** Counting thread states and spotting a lock cycle is mechanical pattern-matching over a large input, which is what Haiku is for. Reserve the expensive reasoning for what the finding *means* about XTDB, which happens back here with the summary in hand.
- **Pass the file path, not the contents.**
- **One agent per dump** where you captured several.

Ask the sub-agent for:

1. **Deadlocks** — threads waiting on locks held by each other. `jstack` usually reports these at the top; take its word and then verify the cycle.
2. **Blocked threads** — what's in `BLOCKED`, and which lock each is waiting for.
3. **Lock contention** — monitors with several waiters, and which thread holds them.
4. **Thread-state distribution** — counts of `RUNNABLE`, `WAITING`, `TIMED_WAITING`, `BLOCKED`.
5. **Thread-pool health** — a pool whose threads are all blocked or waiting is exhausted, and that's usually the cause rather than a symptom.
6. **Repeated stacks** — the same frame across many threads points at the hot path or the shared bottleneck.

And for a report carrying thread names, IDs, lock addresses and the stack excerpts that support each finding — enough that a claim can be checked without reopening the dump.

## Interpreting the result

**Map the findings onto XTDB before recommending anything.**
An allocator or executor name usually identifies the subsystem, and a pool exhausted in one subsystem often means the blockage is in a different one.

**A dump is one instant.** Where the hang is a livelock or a slow leak rather than a true deadlock, take two dumps a few seconds apart and compare — a thread stuck in the same frame across both is a much stronger signal than one sampled once.
