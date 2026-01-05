image::img/xtdb-logo-banner.svg[XTDB Logo]

https://xtdb.com[XTDB] is an open-source immutable SQL database with comprehensive time-travel.
XTDB has been built to simplify application development and address complex data compliance requirements.

Major features:

* Immutable - XTDB is optimised for current-time queries, but you can audit the full history of your database at any point, without needing snapshots or accessing backups.
* 'Bitemporal' - all data is accurately versioned as updates are made ('system' time), but it also allows you to separately record and query when that data is, was, or will become valid in your business domain ('valid' time).
* Dynamic - you don't need to specify schema up-front before documents (rows with arbitrarily nested data) can be inserted.
* Speaks both SQL and https://docs.xtdb.com/xtql/tutorials/introducing-xtql.html[XTQL] - a full SQL dialect that implements the bitemporal functionality as specified in the SQL:2011 standard, available over the Postgres wire protocol.
* Cloud native - the ACID, columnar engine is built on Apache Arrow and designed for object storage
* It is written and supported by https://juxt.pro[JUXT^].

== Quick links

* https://github.com/orgs/xtdb/discussions/4385[Discord Community]
* https://docs.xtdb.com/[Documentation]
* https://github.com/xtdb/xtdb/releases[Release notes]
* Support:
  https://github.com/xtdb/xtdb/discussions[GitHub Discussions^] |
  hello@xtdb.com
* https://repo1.maven.org/maven2/com/xtdb/[Maven releases]
* https://www.zotero.org/groups/4778667/xtdb/[Bibliography^]
* https://github.com/xtdb/xtdb/tree/main/dev[Developing XTDB]
* https://v1-docs.xtdb.com/[XTDB v1 Documentation] (see the `1.x` branch)

== Inside-out Architecture

XTDB embraces a log as the central point of coordination when running as a distributed system.

[quote,Martin Kleppmann]
____
What do we have to gain from turning the database inside out?

Simpler code, better scalability, better robustness, lower latency, and more flexibility for doing interesting things with data.
____

image::docs/public/images/docs/xtdb-node-1.svg[XTDB 2.x Architecture Diagram, 1000, 460]

== Questions, Thoughts & Feedback

We would love to hear from you: hello@xtdb.com

== License and Copyright

XTDB is licensed under the Mozilla Public License, version 2 or (at your option) any later version.

Copyright © 2018-2026 JUXT LTD.
