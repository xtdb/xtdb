# Prior art for the active/passive split

The vocabulary is borrowed rather than invented.
An established name points at a literature you have already read; a private one makes you guess, and the cost of the private version is transmissibility rather than correctness.

## The citations, and what each is doing

- **POSA2 — Schmidt, Stal, Rohnert & Buschmann, *Pattern-Oriented Software Architecture Vol. 2*, 2000.**
  Carries both **Active Object** and **Monitor Object**: one citation, two patterns.
  Passive is the baseline they are defined against, not a third citation.

- **Lavender & Schmidt, *Pattern Languages of Program Design 2*, 1996.**
  The Active Object paper, for the Scheduler-and-`guard()` machinery.
  Optional where POSA2 is cited; dates vary by venue (1995 PLoP workshop, 1996 book).

- **Hoare, "Monitors: An Operating System Structuring Concept", CACM 1974; Brinch Hansen, *Operating System Principles*, 1973.**
  Not decoration — they define the re-evaluable-condition rule that `Watchers`' absorbing `Failed` broke, which is what lets a reader recognise the defect class instead of treating it as a coroutine subtlety.

- **Hoare, "Communicating Sequential Processes", CACM 1978.**
  External choice.
  In CSP external choice (`□`) is resolved by the environment and internal choice (`⊓`) by the process, so a `select` with several clauses is external choice and occam's `ALT` directly.
  **This is why the definition is "offers the choice" rather than "owns a coroutine"**: what the process owns is the *set of events on offer*, and a subordinate exposing an arm is contributing to someone else's offer.

- **Bernhardt, "Functional Core, Imperative Shell", 2012.**
  The closest analogue, and well known in the Clojure sphere XTDB takes inspiration from.
  **Cite it with the substitution spelled out**: same shape, autonomy substituted for purity, and the two axes independent.
  Active ⟹ effectful, but passive ⇏ pure — without that note a reader classifies by side effects and gets the wrong answer.

- **Minsky, "make illegal states unrepresentable"; King, "Parse, Don't Validate", 2019.**
  The principle behind the sealed `Active`/`Failed` pair, and behind why role variants on `LogProcessor` were wrong.

- **Strom & Yemini, "Typestate: A Programming Language Concept for Enhancing Software Reliability", IEEE TSE 1986.**
  Sits with Minsky, dividing the work: Minsky's rule covers *which fields exist*, typestate covers *which operations are legal*.
  XTDB already uses it — `follower_term: FollowerTerm when state = following` in Allium is typestate, and it is checked.
  Kotlin gives the field half via sealed hierarchies and nothing for the operation half, which is worth naming as the gap.

- **Launchbury & Peyton Jones, "Lazy Functional State Threads", 1994.**
  `runST`, the formal name for the `coroutineScope` carve-out: local mutation whose effects provably cannot escape is observationally pure.
  Clojure transients are the version of this that will land in this repo.

## The wider map

Private phrasing on the left, the established name in the middle, the tradition on the right.
Reach for the middle column when writing for an agent.

| what it gets called here | established name | whose |
|---|---|---|
| active = owns a `select` | **Active Object**; CSP external choice / guarded alternative | Lavender & Schmidt, 1996; POSA2, 2000; Hoare, 1978 |
| passive = completes on the caller's thread | passive object | POSA2, 2000 |
| passive + guarded state + condition waits | **Monitor Object** | POSA2, 2000; Hoare & Brinch Hansen, 1974 |
| local mutation that can't escape is still passive | `runST`; Clojure transients | Launchbury & Peyton Jones, 1994 |
| one loop, everything else passive | **functional core, imperative shell** | Bernhardt, 2012 |
| state + services at the boundary | hexagonal architecture / ports & adapters | Cockburn, ~2005 |
| simple vs easy; un-braid, don't complect | *Simple Made Easy* | Hickey, 2011 |
| sum type so fields belong to a state | **make illegal states unrepresentable**; parse, don't validate | Minsky; King, 2019 |
| what operations are legal in this state | **typestate** | Strom & Yemini, 1986 |
| atomicity groups fields into a value | **aggregate boundary** — the unit of consistency | Evans, 2003; Vernon, *Effective Aggregate Design*, 2011 |
| a `Map<Id, State>` beside the objects it identifies | "reference other aggregates by identity" | Vernon, same paper |
| a seam must not cut across a lifetime | ownership / RAII; **structured concurrency** | Rust; Sústrik, 2016; Smith, 2018 |
| states, transitions, who prompts each | **statecharts** | Harel, 1987 |
| derive, don't store | single source of truth; event-sourced projections | Codd onwards |
| spike, see what breaks, revert, land prerequisites first | **the Mikado Method** | Ellnestam & Brolund |
| keep the suite green through the change | parallel change / expand–contract; branch by abstraction | Sato; Hammant |
| hardest implementation first | tracer bullet; walking skeleton | Hunt & Thomas; Cockburn |
| inline it and look, then re-extract | Inline Class, then Extract Class | Fowler's refactoring catalogue |
| equivalence vs behavioural | tidy first; preparatory refactoring | Beck; Fowler |

**What has no published name**: lifetime as a *seam-placement* criterion, region versus value as separate axes, and "does it start a coroutine?" as the category test.

**XTDB has a real ports-and-adapters story** — `Log` is a port, `DurableTotalOrder` (`allium/log-processor-lifecycle.allium`) its contract, and `InMemoryLog`, `LocalLog`, `ReadOnlyLocalLog` and Kafka's `KafkaLog` its adapters.
That is a separate skill's subject, not this one's.
