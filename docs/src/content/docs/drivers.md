---
title: Language Drivers
---

XTDB exposes two client surfaces:

- A PostgreSQL wire-compatible server — works with standard PostgreSQL tools and drivers (psql, JDBC, psycopg, etc.).
- An [Arrow Database Connectivity](/adbc) (ADBC) endpoint, served over Apache Arrow Flight SQL — Arrow batches stream straight into your client, and bulk-ingesting an Arrow table is a single round trip.

For PostgreSQL-shaped tooling, the pgwire path is what you want.

XTDB (unlike some other PostgreSQL wire-compatible databases) does not try to emulate PostgreSQL itself completely, feature-for-feature, bug-for-bug.
XTDB is sufficiently different in certain areas, especially DDL/Schema support, that this is often undesirable (and sometimes impossible!).

That said, the advantage of embracing wire-protocol compatibility is that many PostgreSQL clients or drivers are able to connect to XTDB seamlessly and run many useful queries without issue.

For details of how to connect to XTDB from your favourite language, see the following pages:

- [C](/drivers/c)
- [C#](/drivers/csharp)
- [Clojure](/drivers/clojure) (also Babashka)
- [Elixir](/drivers/elixir)
- [Go](/drivers/go)
- [Java](/drivers/java)
- [Kotlin](/drivers/kotlin)
- [Node.js](/drivers/nodejs)
- [PHP](/drivers/php)
- [Python](/drivers/python)
- [Ruby](/drivers/ruby)

If your pipeline is Arrow-shaped end-to-end (pandas / polars / DuckDB / DataFusion / arrow-rs / pyarrow), you'll want the [ADBC driver](/adbc) instead — it keeps everything Arrow-native and adds bulk-Arrow ingest in one round trip.

XTDB is also compatible with many different PostgreSQL tools, including:

- [psql](https://www.postgresql.org/docs/current/app-psql.html) (PostgreSQL CLI) - connect with `psql -h localhost`
- [SQLTools](https://marketplace.visualstudio.com/items?itemName=mtxr.sqltools) (VSCode extension)
- [Metabase](https://www.metabase.com)
