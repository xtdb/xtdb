# TECHSTACK

The technologies, languages, and libraries XTDB 2.x is built on, grouped by purpose.
Status: current.
This file is the by-purpose map; it deliberately holds no version numbers — exact versions live in `gradle/libs.versions.toml` (and the Gradle version in `gradle/wrapper/gradle-wrapper.properties`).

## Languages & roles

- **Kotlin** — the bulk of `core`: storage, indexing, operators, pgwire, low-level/high-performance runtime.
- **Clojure** — query planner, expression engine, SQL/XTQL, CLI, much of the public API surface.
- **Java** — minimal; ANTLR-generated parser and protobuf code.

## Build & tooling

- **Gradle** multi-module build with a version catalog; `org.gradle.parallel=true` (`gradle.properties`).
- **Clojurephant** (Gradle Clojure), **Kotlin JVM + serialization**, **protobuf**, **Shadow** (uber-jars) plugins.
- **jlink** custom minimal JRE (`build-logic/jlink`) used by all Test/JavaExec/REPL tasks; `-PfullJdk` escape hatch.
- **Dokka** (Kotlin/Java API docs), **Codox** (Clojure API docs), **JReleaser** (Maven Central), **ben-manes versions** (`dependencyUpdates`).
- REPL/AI dev: `clj-nrepl-eval` / clojure-mcp-light (optional), CIDER nREPL, Integrant REPL.

Plugin and tool versions: `gradle/libs.versions.toml` (`[plugins]` and `[versions]`).

## Core libraries by area

Each module's exact dependency set is in its `build.gradle.kts`; versions in `gradle/libs.versions.toml`.
The grouping below is what each area is *for*.

- **Columnar / data**: Apache Arrow (vector, algorithm, compression, memory-netty, flight-sql) + Arrow ADBC; RoaringBitmap, HPPC, Caffeine.
- **Log / messaging**: Kafka clients & connect-api; Protobuf; Transit (clj/java); kotlinx-coroutines.
- **SQL / parsing**: ANTLR; pgjdbc; next.jdbc, HoneySQL, JDBI, Exposed; pg2 (pgwire client, testing).
- **HTTP**: Ring + ring-jetty9-adapter, Reitit, Muuntaja, Jsonista, Hato (healthz / HTTP surfaces).
- **Object stores**: AWS SDK (S3, CloudWatch, STS), Azure (storage-blob, identity, management), Google Cloud Storage + Guava.
- **Clojure core**: spec, data.json, data.csv, tools.cli, clj-yaml, Integrant.
- **Kotlin serialization**: kotlinx-serialization-json, kaml.
- **Logging**: SLF4J facade over Log4j2 (jul/jcl/jpl/slf4j bridges, JSON layout); clojure.tools.logging.
- **Observability**: Micrometer (+ Prometheus / CloudWatch / Azure Monitor registries), Micrometer Tracing, OpenTelemetry SDK (OTLP exporter).
- **Testing**: JUnit Jupiter, clojure.test.check, Testcontainers (kafka/postgres/minio/keycloak), Kotest, MockK.

## Runtime requirements

- JDK 21+ (enforced by the Gradle toolchain).
- Off-heap direct memory for Arrow (Netty allocator); the JVM runs with `--enable-native-access`, `--add-opens java.base/java.nio`, and a configured `MaxDirectMemorySize` (see root `build.gradle.kts` for the exact flags).
- A log (Kafka or local) and an object store (S3/Azure/GCS/local).
