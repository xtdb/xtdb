---
title: Language Drivers
---

XTDB exposes a PostgreSQL wire-compatible server, and is therefore compatible with standard PostgreSQL tools and drivers.

XTDB (unlike some other PostgreSQL wire-compatible databases) does not try to emulate PostgreSQL itself completely, feature-for-feature, bug-for-bug.
XTDB is sufficiently different in certain areas, especially DDL/Schema support, that this is often undesirable (and sometimes impossible!).

That said, the advantage of embracing wire-protocol compatibility is that many PostgreSQL clients or drivers are able to connect to XTDB seamlessly and run many useful queries without issue.

For details of how to connect to XTDB from your favourite language, see the following pages:

- [Clojure](/drivers/clojure)
- [Elixir](/drivers/elixir)
- [Java](/drivers/java)
- [Kotlin](/drivers/kotlin)
- [Node.js](/drivers/nodejs)
- [Python](/drivers/python)

XTDB is also compatible with many different PostgreSQL tools, including:

- [psql](https://www.postgresql.org/docs/current/app-psql.html) (PostgreSQL CLI) - connect with `psql -h localhost`
- [SQLTools](https://marketplace.visualstudio.com/items?itemName=mtxr.sqltools) (VSCode extension)
- [Metabase](https://www.metabase.com)
