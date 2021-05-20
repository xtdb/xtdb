## Principles

- Multiple nodes tailing same log, sharing the same object store.
- Dependency tree to derive immutable chunks (with revisions) from each other.
- Declare data dependencies instead of building them. Especially don't do unexpected I/O.
- Persistent data sits in the object store (or log), transient data in memory or disk.
- Evict handled via new chunk version + GC.
- Non-blocking I/O everywhere it makes sense.
- Compiled queries operating on vectors/batches.
- JMH for microbenchmarks of CPU-bound pieces.
- Always also compare CPU-bound pieces with Java.
- Prefer property based testing, including stateful testing.
- API for end-to-end.
- If it gets hairy, take a step back. Avoid locks or synchronisation for example.
- Use of local in-memory Clojure state is discouraged.
- Take a break to read and review papers, avoid NIH.
- Use Java interfaces and types between subsystem boundaries.
- Focus on data, not code.

Data:
- Arrow columnar format everywhere.
- Nippy Arrow-extension type to bootstrap, preference is to later remove it.
- RoaringBitmap Arrow-extension type.
- Arrow used for off-heap memory management.

Two main extension points:
- Single log feeding transactions (but not tied to this usage).
- Shared object store for persistence.

## Problems with Classic

- All storage on every node
- Infinite retention on the log
- Imposing Kafka but not really benefiting from it
- I/O chatty query engine - depends on sorting + hashing
- Designed around timeslice queries - no straightforward way to go to advanced bitemp functionality
- Local maximum in ingest/query performance
- Overly flexible data model

### End scenarios:
- Core2 as a viable alternative to Crux
  Spectrum:
  - A: Drop-in replacement for Crux
  - B: Essentially Crux but with Arrow
  - C: Crux re-thought
- Core2 canned, knowledge pulled into Crux

### Things to consider:
- User migration?
- Maintaining Crux
- Pulling existing users over vs attracting new users

## Risks to Core2:
- Performance
  - Core2 requires scanning everything, queries too slow.
    Mitigated by TPC-H SF, WatDiv
  - Cold caches - low latency queries require remote data transfer
  - Buffer pool doesn't evict anything
  - Can't get temporal index working remotely, append-only
  - By making advanced temporality possible, we forego low-latency performance for as-of-now queries
- Something becomes infeasible to represent in Arrow
- Duplication of ingest work becomes prohibitively expensive
- Expensive to run in cloud
- Stuck trying to replicate Classic

### Non-technical risks:
- Resourcing - scaling team and remaining productive
- External buy-in and understanding, getting people back on board
- External expectation of drop-in replacement
- Crux rename timing
- Classic vs Core2 time/focus
- Keeping the Classic flame alive while we're working on Core2

## Deliverables

1. Go/No-Go for Storage/Compute + Arrow
   - Large TPC-H SF (1?), running remotely, hot/cold - performance numbers, billing, monitoring, bottlenecks
     - DONE Kafka, S3
     - Some level of deployment/monitoring
       - DONE create uberjar
     - DONE Full TPC-H
     - Larger TPC-H scale factors (SF10) - check ingest + query
       - linear growth in aggregate queries, sub-linear in accesses
     - Concurrency - check running ingest + multiple queries in parallel
       - DONE Running multiple TPC-H nodes in the cloud - check everything works same as locally
         works with SF0.01, super-linear ingest issues currently prevent SF0.1
     - Check queries too slow, possible solutions
       - DONE Bloom filters
       - DONE Block-level metadata
       - Predicate push-down
         - DONE for joins passing bloom filters
     - DONE Upgrade to Arrow 4.0.0
       - Investigate Dataset API?
     - Check cold caches, possible solutions:
       - Tiered caching
   - Join order benchmarking - WatDiv, graph
     - WCOJ? see worst-case optimal hash join paper.
       - constructing/storing hash indices?
   - Dealing with updates over time - historical dataset (TS Devices)
     - DONE ingest bench
     - DONE Temporal range predicates in logical plan scan
     - TODO test the queries using our logical plan
   - Scalable temporal indexing + querying
     - Exercise temporal side, TPC-BiH
     - DONE SQL:2011 period predicates in logical plan expressions:
       - overlaps, equals, contains, (immediately) precedes, (immediately) succeeds.
     - DONE Add interval types and arithmetic?
     - How far does the current kd-tree take us? Need different approach or fixable as initial cut?
   - Bigger than local node databases
     - DONE Buffer pool eviction and size limit.
     - DONE Ability to query several temporal chunks (live and Arrow).
     - DONE Merging of temporal Arrow chunks?
     - DONE Move merging of the snapshot temporal tree to the background.
2. Core2 as something keen users can play with
   - Features, functionality
     - Higher-level queries
       - Multi-way WCOJ hash joins?
       - EDN Datalog.
       - SQL.
   - Documentation
3. Core2 as a viable alternative to Crux
   - Deployment, monitoring
     - multi-module (Kafka, S3)
   - Documentation, marketing
   - Features, functionality
     - Bitemporal features, interval algebra.
     - Eviction
     - More logs/object-stores
       - Kinesis
       - GCP Pub/Sub and Cloud Storage.
       - GCP benchmarks.
       - Azure EventHubs and Blobs.
       - Azure benchmarks.
       - JDBC log, object store.
   - Migration from Classic
   - How much of Crux should Core2 pull in?

Clean up:
- TODO remaining late-mat optimisations
  - asymmetry of Dates?
  - confirm effect of using extra heap - can we reduce this?
  - reduce footprint of LinkedHashMap implementation detail
- Parameterisation
  - TODO Ensure metadata can use calculated literals (e.g. `?date + INTERVAL "3 months")`)
  - TODO Ensure `like` can accept dynamic values in the needle argument
- DUV completeness (removal of some DUV hacks/pain)
- Clean up data model: Java/Clojure types, Java Arrow types, representation of types (keywords, ArrowType, class etc.), conversion between types.
- Review memory-management.
- JMH - how to leverage, still needed?

Should have:
- External sort in query engine
- We don't currently coalesce small intermediate blocks in the query engine
- DONE Parameters beyond table-operator - override values in expressions without recompiling.
  - DONE Change TPC-H queries to use parameters.
- Bringing external CSV/Arrow into Core2?
  - Will need to remove assumptions around type-ids in the logical plan/query engine
- Support lists/cardinality-many? Requires discussion
- Reconstructing the log - storage of incoming transactions in object store
- Tx fns?
- Speculative transactions?

Misc functionality/ideas:
- GHD-based planner?
- Option C content - inspiration from Truffle's DynamicObjects/Shapes
- Graphy? Some kind of 'aggregate join' so that your query can return a tree?
- Event Sourcing, Data Mesh story
- Arrow - keep adding utilities or rewrite the parts we need (memory management a risk)?
- Ditch Arrow in-favour for a bespoke, dynamic-first columnar data model, similar to timeline-spike (which uses Flexbuffers for documents)?
- Schema-aware operators, currently requires queries to see schema.
- Ingest and chunk dependency system. We want the query engine to avoid generating its dependent data.
- Partitioning/sharding
