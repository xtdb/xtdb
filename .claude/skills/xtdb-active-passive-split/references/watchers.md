# Worked example — `Watchers` is a lock-free Monitor Object

`core/src/main/kotlin/xtdb/api/log/Watchers.kt`.

It carries three things the taxonomy exists to make visible: why a lock-free monitor reads as ordinary passive state, why a latching condition is a *monitor* defect rather than a coroutine subtlety, and why two populations of waiter were never one value.

## Why it passes for inert

**A `MutableStateFlow<State>` over a sealed `Active`/`Failed` pair, with `updateIfActive` as the single write path — `notifyApplied` and `notifyError` both go through it.**
That is a CAS plus a wait-list of continuations — there is no `synchronized` block, no `ReentrantLock`, no `Condition`, so nothing in the file looks like the monitor it is.

Run the test instead of reading for a lock: **does anyone wait on it?**
`awaitReplicaMsg`, `awaitTx` and `awaitSource` each suspend on `activeState.first { … }`, which is a condition wait over state that outlives the call.
Three condition waits over state guarded by one write path is a monitor, whatever the notify methods are called.

## The absorbing-`Failed` bug is a monitor defect

**`Failed` latches.**
`updateIfActive` returns `it` unchanged when the state is already `Failed`, and `activeOrThrow` rethrows for every reader, so a database that failed ingestion once stays unqueryable until the node restarts — a routine handover left a queryable database unqueryable.

**That breaks the re-evaluable-condition rule directly.**
A monitor's condition MUST admit a waiter arriving later reaching a different verdict; a latch guarantees it cannot.
Hoare (1974) and Brinch Hansen (1973) are what make this a named defect class rather than a one-off.

**The primitive was never the fault.**
`Failed` would latch identically behind a `ReentrantLock` and a `Condition`, because the latch is in the state machine, not in the waiting mechanism.
A fix that reaches for a different concurrency primitive is fixing the wrong layer.

## The waiters are two populations

They were collapsed into one type because they share a condition, not because they share a mechanism.

- **Parked threads.**
  `Database.awaitTxBlocking` and `awaitSourceBlocking` (`Database.kt:126`, `:132`) are `runBlocking` around the suspend functions, and they are what `Xtdb.kt:327` and `log.clj:89` call.
  A parked thread either way, so locks would be simpler and more honest for this half.

- **Genuinely suspending callers.**
  `LogProcessor.awaitReplicaMsg` (`LogProcessor.kt:285`), `Compactor.awaitSource` (`Compactor.kt:147`) and `Database.sync()` (`Database.kt:524`).
  The leader transition's catch-up await MUST cancel promptly, which `Condition.await` cannot give — so this half is why the coroutine machinery is there at all.

**The consequence for a diff:** a change that simplifies `Watchers` toward locks has to account for the second population's cancellation, and a change that pushes further into flows has to account for the first population still being threads.
Neither half is removable by preference.
