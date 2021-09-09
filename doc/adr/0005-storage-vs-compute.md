# 5. Storage vs compute

Date: 2021-09-09

## Status

Accepted

## Context

XTDB is a deterministic database based on a single log of proposed
transactions. Each node indexes the log independently and serve
queries.

We want to avoid each node to store its own local index. This is to:

1. Avoid duplicating the index on each node.
2. Make nodes easy to add and remove without replaying the full log.
3. Make the full index able to far exceed each node's local disk size.

## Decision

The index is written in chunks to a shared object store. As these
chunks are a function of the log, all nodes will reach the same
conclusion and upload the same immutable chunks to the object store.

We will support two simple extension points:

1. Object store. This is a eventually consistent key/value store, like
   for example S3.
2. Log. This is a totally ordered log of proposed transactions, like
   for example a Kafka topic with a single partition.

## Consequences

Nodes will be tailing the log, and all persistent data is stored in
the shared object store. Nodes will cache immutable chunks of data
locally for query processing.

Nodes can come and go elastically in the cluster and will only have to
catch up from the latest chunk.

### Known issues

1. The design leads to rework across nodes.
2. [Eviction](0004-eviction.md) breaks the deterministic and immutable
   model.
