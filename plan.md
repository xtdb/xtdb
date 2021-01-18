Principles:

Multiple nodes tailing same log, sharing the same object store.
Dependency tree to derive immutable chunks (with revisions) from each other.
Declare data dependencies instead of building them. Especially don't do unexpected I/O.
Persistent data sits in the object store (or log), transient data in memory or disk.
Evict handled via new chunk version + GC.
Non-blocking I/O everywhere it makes sense.
Compiled queries operating on vectors/batches.
JMH for microbenchmarks of CPU-bound pieces.
Always also compare CPU-bound pieces with Java.
Prefer property based testing, including stateful testing.
API for end-to-end.
If it gets hairy, take a step back. Avoid locks or synchronisation for example.
Use of local in-memory Clojure state is discouraged.
Take a break to read and review papers, avoid NIH.
Use Java interfaces and types between subsystem boundaries.
Focus on data, not code.

Data:
Arrow columnar format everywhere.
Nippy Arrow-extension type to bootstrap, preference is to later remove it.
RoaringBitmap Arrow-extension type.
Arrow used for off-heap memory management.

Two main extension points:
Single log feeding transactions (but not tied to this usage).
Shared object store for persistence.

--- Storage vs Compute, Hakan and James (maybe Jon?) Q1

Milestone 1: Ingest and Data Access
Ingest and chunk dependency system. We want the query engine to avoid generating its dependent data.
Multiple local nodes sharing local file storage.
Eviction.
Code-level queries, basic relation operators.
Transaction timeslice index. We want to capture the context that some rows might not be valid at T.
MVCC based on above.

Milestone 2: Cloud and Benchmarks (maybe Dan? Matt?)
Kafka/Kinesis and S3.
AWS benchmarks.
TPC-H.
Watdiv.
Subset of EDN Datalog, WCOJ.

--- Temporal Q2

Milestone 3:
Temporal indexing.
EDN Datalog, the good parts (rules etc.).
Subset of TPC-BiH.

--- Alpha, wider team Q2/Q3

Milestone 4: Multi-cloud, operations (most of team)
Tx fns.
Speculative txs.
GCP Pub/Sub and Cloud Storage.
GCP benchmarks.
Azure EventHubs and Blobs.
Azure benchmarks.
Kubernetes.
JDBC log, object store.
Monitoring and metrics.
Documentation.
Announcement.

Milestone 5: Q3/Q4
Defined data model and public APIs.
Parity with parts of Crux we want to keep.
Migration.
