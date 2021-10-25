# State of C2

Many of the points here are dealt with in more detail in various ADRs,
but there's no consolidated place to see the big picture.

## Core

This is parts shared between SQL and Datalog. A lot of "hidden" scope
sits here.

### Query engine

The hope is to postpone most planner work until post v1.0, but that's
highly dependent on how it behaves during testing and if we can get
away with tuning the raw speed.

#### Expression compiler

- Full Arrow type system support.
- Nested operators.
- Intermediate results.
- Three-valued logic.
- Nulls.

#### Operators

- Align group-by, order-by and set operators with full Arrow.
- Equi-joins over more than one column.
- Path-expression operators for SQL/PGQ?

#### Planner

- Can we really avoid cost-based planning?
- Other query rewrite rules?

#### Transaction processing

- Need conditional transactions (match, constraints).
- Boils down to some form of deterministic function processing during
  transactions.

### Storage vs compute

#### Temporal indexing

- Need to deal with same millisecond transactions.
- Better snapshots.
- Ingest efficiency.
- Testing at scale.
- Good enough for 1.0?

#### Eviction

- Proposal exists, needs implementation and testing.

#### Efficiency

- Can we get away with no indexing?
- Cache hierarchy, populating and managing.
- Local tuning, fighting Clojure.

## Testing

C2 is a new engine, and will require a lot of testing before we can
comfortably try to sell it. Some of this can be done under
tech-previews, alphas and betas, but we need more knowledge ourselves
before that.

See also: Core, SQL.

### Functional

- Need more test suites, real deployments, use-cases.

### Benchmarks

- TPC-H, TPC-BiH (subset), WatDiv, Join-order-benchmark? More?
- Ingest and transaction benchmarks.

## SQL

We aim to provide a SQL:2011 based dialect that works well with schema
on demand and temporal, while also (for debate) providing basic
graph/path queries as per SQL/PGQ.

See also: Testing, Core: Expression compiler.

### Generation of logical plan

- Walk tree, generate working logical plan.
- Rewrite logical plan to remove dependent joins (sub-query unnesting).
- Can we get away with no other rewrite rules?

### Functions and semantics

- Need to implement all the SQL:2011 functions within scope.
- Clean semantics to deal with schema on demand.
- May need to extend temporal side slightly for easy-of-use.
- Will SQL/PGQ be in scope?
- Will need qualified columns and select star may need further work
  due to how we shred documents. Currently there's no way of knowing
  which columns exist for a specific row id, nor a way to expose the
  row id out to the query engine as a virtual column.

## Datalog

Datalog support can be seen as a spectrum:

- SQL semantics with Datalog syntax.
- Classic semantics.

Maybe obviously, the closer we move to classic semantics, the more
work there will be, and the closer to stay to SQL semantics, the
larger the difference to Classic it will have.

The handicap here when it comes to build it, ignoring at the product
and strategy level in this context, can be broken down into a few
parts:

- Need to take Datalog into account in the core, deal with trade-offs,
  which leads to a lack of focus.
- Cannot ignore it fully even if the boundary is clear and the module
  is delegated away, and be prepared to task-switch and analyse it.
- Always the risk that the boundary will be decided to move suddenly,
  this leads to uncertainty.
- There's a lot of work in the other parts which needs to happen
  regardless and will really stretch the team.
- The query engine won't work like classic in any case, and without a
  lot of work necessarily perform well, so it's never going to be an
  easy win, if not done well, it may hurt us.
- Will need to maintain and fix bugs. Need to ensure this happens with
  respect to the core and SQL side which requires understanding of the
  full system. The flip-side of this applies to the SQL-side as well,
  a simple fix may break Datalog, which makes it harder.

One can summarise my worry around the handicap is that Datalog won't
be like a new object store implementation, it will create connections
and dependencies, explicit or implicit, to almost all parts of the
system, which greatly increases the number of things one has to keep
in mind at all times in an already complex system with many
interlocking variables.

See also: SQL, Core: Expression compiler, Core: operators.

### Semantic differences from Classic

The core is easiest built with a single semantics. As I propose we use
SQL's, this implies three-valued logic and a limited set clearly
defined functions provided by the query engine, and not calling
arbitrarily Clojure inside the query.

Classic relies heavily on Nippy and the Clojure type-system, so there
will be some differences here as well. The Arrow type system provides
extension types so it's possible to add a known set, like the basic
EDN types, but one cannot cleanly deal with arbitrarily Java objects
in a useful way except having them flow through as bytes.

### Cardinality-many

In classic semantics, this requires us to unwind all lists early on,
and then the engine will work closer to classic. With SQL semantics,
lists would have to be unwound manually inside the query using a
function.

### Sub-queries (or-join/not-join) and rules

This is non trivial to build in an efficient way using the logical
plan and operators we have. Simple cases will be simple to map, like
with single variables, or rules where there's no recursion.

But the full power of Datalog will require work, and to be really
efficient, likely work inside the core. One can introduce n-plus-one
operators that execute a sub-query for each set of bound variables to
potentially make this easier, but that's less efficient.

### Three-valued logic

SQL uses three-valued logic, unlike Clojure where a nil is false.

### Clojure functions

Because of the lower-level of the expression engine and it being tied
to the Arrow type system, providing access to arbitrary Clojure
functions within the query, while possible, isn't a good fit. It's
possible to open this up with a generic and slower mapping fallback
function, but for other reasons my preference is to not allow calling
arbitrary code inside the engine.

### Pull

Same issue as select star in SQL, needs work or limitations
imposed. See above.
