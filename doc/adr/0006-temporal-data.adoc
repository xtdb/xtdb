# 6. Temporal data

Date: 2021-09-09

## Status

Proposed

## Context

XTDB is a temporal database that needs to support temporal query
capabilities beyond time-slice queries. Range queries across both
transaction and valid time must be supported.

## Decision

We will support [SQL:2011 temporal
semantics](https://standards.iso.org/ittf/PubliclyAvailableStandards/c060394_ISO_IEC_TR_19075-2_2015.zip)
under the hood. Advanced temporal queries should be possible, but not
necessarily highly optimised. Time-slice queries must still be
competitive.

Internally, we will base the temporal data model on the [Bitemporal
Conceptual Data
Model](https://www2.cs.arizona.edu/~rts/pubs/ISDec94.pdf) from Jensen,
Soo and Snodgrass.

## Consequences

Requires us to implement an advanced temporal index alongside the
columnar chunks and expose this indirectly to the query engine.

We should start using SQL:2011 terminology instead of Snodgrass
everywhere for consistency:

- Transaction time - system time.
- Valid time - application time.
