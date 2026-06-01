# CODEMAP

Map of the XTDB 2.x workspace: modules, where each concern lives, and how the source tree is laid out.
Status: current.

## Build topology

Gradle multi-module build (JDK 21, version catalog at `gradle/libs.versions.toml`).
`build-logic/jlink` is an included build that produces the minimal custom JRE used by all `Test`/`JavaExec`/`clojureRepl` tasks.
`buildSrc` holds custom plugins (Shadow-jar config, `DataReaderTransformer`).
The authoritative list of modules and their `xtdb-*` artifact names is `settings.gradle.kts`; the table below adds each module's purpose.

## Modules

| Module | Dir | Artifact | Languages | Purpose |
| --- | --- | --- | --- | --- |
| api | `api/` | xtdb-api | Clojure, Kotlin, proto | Public API, error types, transit serde, Arrow integration |
| core | `core/` | xtdb-core | Kotlin, Clojure, Java, ANTLR, proto | Query engine, storage, indexing, compaction, log coordination, pgwire, SQL grammar, monitoring |
| main | `main/` | xtdb-main | Clojure | CLI entrypoint, playground, logging config |
| kafka | `modules/kafka/` | xtdb-kafka | Clojure, Kotlin, proto | Kafka source/replica log backend |
| kafka-connect | `modules/kafka-connect/` | xtdb-kafka-connect (labs) | Clojure, Kotlin | Kafka Connect sink connector (shadow-jar) |
| postgres-source | `modules/postgres-source/` | xtdb-postgres-source | Kotlin, proto | Postgres CDC external source |
| aws | `modules/aws/` | xtdb-aws | Clojure, Kotlin, proto | S3 object store + CloudWatch metrics |
| azure | `modules/azure/` | xtdb-azure | Clojure, Kotlin, proto | Azure Blob store + Azure Monitor |
| google-cloud | `modules/google-cloud/` | xtdb-google-cloud | Clojure, Kotlin, proto | GCS object store |
| datasets | `modules/datasets/` | xtdb-datasets | Clojure | TPC-H / benchmark data generation |
| bench | `modules/bench/` | (unpublished) | Clojure | Benchmarks (TPC-H, auctionmark, scan-perf, tsbs, …) |
| docker:standalone | `docker/standalone/` | xtdb-standalone | Java | Single-jar distribution image |
| docker:{aws,azure,google-cloud} | `docker/*/` | (unpublished) | config | Cloud-variant Docker images |
| monitoring(:docker-image) | `monitoring/` | — | config | Local Prometheus/Grafana/Tempo stack (conditional) |
| lang:test-harness | `lang/test-harness/` | test-harness | — | Non-JVM language test harness (conditional include) |

## Concern → code-area map

- Public API & config: `api/src/main/kotlin/xtdb/api/`, `api/src/main/clojure/xtdb/`
- Error handling: `api/src/main/clojure/xtdb/error.clj`, `api/src/main/kotlin/xtdb/error/Anomaly.kt`
- Indexing & log coordination: `core/src/main/kotlin/xtdb/indexer/` (LiveIndex, LogProcessor + Leader/Follower/ExternalSource/Transition variants, BlockUploader)
- Catalogs (State): `core/src/main/kotlin/xtdb/catalog/`, Clojure `xtdb/{block_catalog,table_catalog,trie_catalog}.clj`
- Storage: `core/src/main/kotlin/xtdb/storage/` (BufferPool, RemoteBufferPool), `core/src/main/kotlin/xtdb/block/`
- Compaction / GC: `core/src/main/clojure/xtdb/compactor.clj`, `xtdb/garbage_collector.clj` (+ `dev/doc/compaction.allium`, `gc.allium`, `block-gc.allium`)
- Query engine: `core/src/main/clojure/xtdb/logical_plan.clj`, `core/src/main/kotlin/xtdb/operator/`, expression engine `xtdb/expression.clj` + `core/src/main/kotlin/xtdb/expression/`
- SQL: ANTLR grammar in `core/src/main/antlr/`, parse in `core/src/main/clojure/xtdb/sql/`
- XTQL: `core/src/main/clojure/xtdb/xtql/`
- pgwire / Flight SQL: `core/src/main/kotlin/xtdb/pgwire/`, `flight_sql/`
- Bitemporal resolution: `core/src/main/kotlin/xtdb/bitemporal/`, scan in `core/src/main/kotlin/xtdb/operator/scan/`

## Source & test layout

- Main source per module: `src/main/{clojure,kotlin,java,resources}` (core adds `src/main/{antlr,proto}`).
- Most Clojure tests live in the **root** module under `src/test/clojure` (run via root `:test`).
- Kotlin tests live per-module under `src/test/kotlin` (e.g. `:xtdb-core:test`).
- Shared test fixtures: `src/testFixtures/clojure`, with `src/testFixtures/resources/log4j2-test.xml`.

## Submodules

External, vendored — not project doc roots (source of truth: `.gitmodules`):
- `docs/shared` — shared website assets.
- `docs/lib/railroad` — railroad-diagram generator.
- `modules/datasets/lib/tsbs` — Time Series Benchmark Suite.

## Existing developer docs (pre-Rosetta, authoritative)

- `AGENTS.md` — agent/dev conventions (root).
- `dev/README.adoc`, `dev/GIT.adoc` — developer workflow & git practices.
- `dev/doc/*.adoc` — architecture (`high-level-tour.adoc`, `query_and_access_patterns.adoc`, `evaluating-index-strategies.adoc`).
- `dev/doc/*.allium` — executable domain specs (`db`, `compaction`, `gc`, `block-gc`, `trie-cat`).
- `docs/` — end-user Astro documentation site.
