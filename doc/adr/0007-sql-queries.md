# 7. SQL queries

Date: 2021-09-09

## Status

Proposed

## Context

XTDB should be possible to query via SQL. We need support for temporal
queries and a natural way to access nested data in documents.

### Implementation

The SQL implementation is responsible for parsing SQL queries and to
generate a logical plan. It's not responsible for optimising the plan
or the translation of it into a physical plan.

### Parser

Options:

1. Use existing framework or library, like
   [Calcite](https://calcite.apache.org/),
   [Presto](https://prestodb.io/) or
   [H2](https://www.h2database.com/html/main.html).
2. Leverage existing parser, like [PartiQL](https://partiql.org/) or
   [JSQLParser](https://github.com/JSQLParser/JSqlParser).
3. Write our own parser for the parts we need using
   [Instaparse](https://github.com/Engelberg/instaparse),
   [Antlr](https://www.antlr.org/) or handwritten recursive descent.

### API

Options:

1. Our own JDBC driver.
2. [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
   (gRPC). SQL extension in progress.
3. [PostgreSQL wire
   protocol](https://www.postgresql.org/docs/current/protocol.html).
4. HTTP (using Arrow IPC streaming mime-type).

### Nested data

Options:

1. [SQL:1999](https://crate.io/docs/sql-99/en/latest/) structured
   user-defined types.
2. [SQL:2016
   JSON](https://standards.iso.org/ittf/PubliclyAvailableStandards/c067367_ISO_IEC_TR_19075-6_2017.zip).
3. PartiQL "SQL-compatible access to
   relational, semi-structured, and nested data."

### Graph

Options:

1. SQL:1999 common table expressions and recursive queries.
2. [SQL:202x/PGQ](https://s3.amazonaws.com/artifacts.opencypher.org/website/ocim5/slides/ocim5+-+SQL+and+GQL+Status+2019-03-06.pdf)
   "SQL Property Graph Query",
   [openCypher](https://opencypher.org/)-like queries embedded in
   SQL. Related to [GQL](https://www.gqlstandards.org/) which is a new
   language (and not to be confused with GraphQL).

### Temporal

Options:

1. [SQL:2011
   temporal](https://standards.iso.org/ittf/PubliclyAvailableStandards/c060394_ISO_IEC_TR_19075-2_2015.zip).
2. [TSQL-2](https://www2.cs.arizona.edu/~rts/tsql2.html) "TSQL2 is a
   temporal extension to the SQL-92 language standard." More advanced,
   but never made it into SQL, superseded by the above.
3. Design our own extensions.

## Decision

We will support:

1. A large part of
   [SQL-92](https://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt).
2. The temporal part of SQL-2011, potentially with our own extensions.
3. Structured user-defined types (nested access) and booleans as of
   SQL-1999.
4. Take inspiration from PartiQL.

XTDB should not require a schema to work, so columns in SQL will need
to qualified.

Queries should be possible to execute via HTTP or the PostgreSQL wire
protocol. JDBC is supported via existing PostgreSQL drives. The Arrow
IPC streaming format has its own mime-type we will support, but should
also support different representations like JSON, EDN, CSV.

We will write our own parser, closely following the [official SQL:2011
grammar](https://jakewheat.github.io/sql-overview/sql-2011-foundation-grammar.html),
but avoiding parts we don't support.


## Consequences

All processing needs to map down to the internal [data
model](0002-data-model.md) and [temporal data](0006-temporal-data.md)
support.

In the core, the logical plan will need to expand to support the parts
of SQL we need. The expression evaluator needs to support three-valued
logic like SQL. We also need to add a reasonable set of SQL predicates
and functions to the expression evaluator.

The PostgreSQL wire protocol doesn't support representing the full
Arrow type system. A column in the result set has a single type and
there's no concept of unions. The type system can be extended, but the
question is what happens when using existing drivers.

We're currently postponing any decisions around dealing with graph
queries to a later date.
