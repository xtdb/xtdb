# 6. Temporal data

Date: 2021-09-09

## Status

Proposed

## Context

XTDB is a temporal database that needs to support temporal query
capabilities beyond time-slice queries. Range queries across both
transaction and valid time must be supported.

## Decision

We will support SQL:2011 temporal semantics under the hood. Advanced
temporal queries should be possible, but not necessarily highly
optimised. Time-slice queries must still be competitive.

## Consequences

Requires us to implement an advanced temporal index alongside the
columnar chunks and expose this indirectly to the query engine.
