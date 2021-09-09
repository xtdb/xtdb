# 7. SQL queries

Date: 2021-09-09

## Status

Proposed

## Context

XTDB should be possible to query via SQL.

## Decision

We will support:

1. A large part of SQL-92.
2. The temporal part of SQL-2011.
3. Structured user-defined types (nested access) and booleans as of
   SQL-1999.
4. Take inspiration from [PartiQL](https://partiql.org/):
   "SQL-compatible access to relational, semi-structured, and nested
   data." .

XTDB should not require a schema to work, so columns in SQL will need
to qualified.

## Consequences

All processing needs to map down to the internal [data
model](0002-data-model.md).
