# 3. Backwards compatibility

Date: 2021-09-09

## Status

Superceded by:

* Tx Fns: [ADR-0011](0011-classic-tx-fns.md)
* Lucene: [ADR-0012](0012-classic-lucene.md)
* Pull Syntax [ADR-0013](0013-classic-pull.md)
* Speculative transactions [ADR-0014](0014-classic-speculative-txes.md)
* Clojure predicates [ADR-0015](0015-classic-clojure-predicates.md)
* Arbitrary Types [ADR-0017](0017-classic-arbitrary-types.md)
* Datalog Rules [ADR-0018](0018-classic-datalog-rules.md)
* Advanced Datalog [ADR-0019](0019-classic-advanced-datalog.md)
* Migration Tool [ADR-0020](0020-classic-migration-tool.md)
* Tx-ops [ADR-0021](0021-classic-tx-fns.md)
* History API [ADR-0022](0022-classic-history-api.md)

## Context

XTDB needs some level of compatibility with classic. At the minimum we
need to import the data and timeline.

### Transaction processing (write side)

#### Rollback

Currently c2 takes a zero-copy of the live data and present that for
queries. The easiest way to do rollbacks is to do this the other way
around. Have the transaction take a zero-copy slice, modify it (append
only) and then if it commits, replace and release (decrease the
reference count) of the previous watermark.

The temporal in-memory index is a persistent data structure, so you
wouldn't need to do anything here, just go back to the previous
version.

#### See Also

* [Eviction](0004-eviction.md), assumed to be orthogonal.

## Decision

N/A. This ADR has been superceded as it is too all-encompassing. We
will need to decide on a case-by-base basis what elements of classic
compatibility we want to commit to.

## Consequences
