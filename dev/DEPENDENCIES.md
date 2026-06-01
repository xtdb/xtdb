# DEPENDENCIES

How the modules depend on each other and which external system each integrates with.
Status: current.
Exact dependency declarations live in each module's `build.gradle.kts`; versions in `gradle/libs.versions.toml`.
This file holds only the wiring that isn't obvious from a single build file.

## Inter-module dependencies

- `xtdb-core` depends on `xtdb-api`.
- `xtdb-main` depends on `xtdb-core` (+ logging).
- Each backend module (`kafka`, `aws`, `azure`, `google-cloud`, `postgres-source`) depends on `xtdb-core`/`xtdb-api` and plugs in a log or object-store implementation.
- Docker image projects aggregate `xtdb-main` + the relevant backend modules via shadow-jar.
- `bench` / `datasets` depend on core + cloud SDKs for workload generation.

## Module → external system

What each module talks to at runtime (the part not visible in a single `build.gradle.kts`).
For the actual libraries, read that module's `build.gradle.kts`.

| Module | Integrates with |
| --- | --- |
| xtdb-api | SQL clients, columnar (Arrow) serde |
| xtdb-core | pgwire, healthz HTTP, metrics/tracing export, SQL parsing |
| xtdb-main | CLI / process entrypoint |
| xtdb-kafka | Apache Kafka (source + replica log) |
| xtdb-postgres-source | PostgreSQL logical replication (CDC external source) |
| xtdb-aws | AWS S3 (object store), CloudWatch (metrics) |
| xtdb-azure | Azure Blob Storage, Azure Monitor (metrics) |
| xtdb-google-cloud | Google Cloud Storage |
| xtdb-kafka-connect | Kafka Connect |
| modules:bench | benchmarking; all cloud backends |
| modules:datasets | benchmark dataset generation (TPC-H, cloud-sourced) |

## Vendored submodules

See `.gitmodules` (also mapped in `dev/CODEMAP.md`) — fetched via git submodule + git-lfs where applicable.

## Maintenance

`./gradlew dependencyUpdates` reports available upgrades against the version catalog — see the "Tooling" section of `dev/README.adoc`.
