# 4. Eviction

Date: 2021-09-09

## Status

Proposed

## Context

XTDB is a temporal database that keeps all history around. For certain
cases, mainly GDPR, we need to be able to do hard deletes, or
"evictions" of data.

XTDB is further a deterministic database where the state is derived
from single source of truth - the log of proposed transactions. But
while XTDB nodes work independently, they share the same eventually
consistent object store for persistence.

Eviction introduces distributed systems issues which XTDB is otherwise
designed to avoid.

### Eventual consistency example

Trying to boil down to a simple example, claiming a unique user, based
on a name field (not the id) for good measure:

- user is initially claimed at tx-id 3, tried to be reclaimed at 4 and
  failed, evicted at 5.
- slower node reaching reclaim at 4 after name field column for has
  been eventually evicted in the object store, this claimed row from
  tx-id 3 is still visible in the local kd-tree, but that doesn't
  matter - the data itself is gone so check returns nothing, so
  wrongly assigns name to different user in tx 4 (which failed claim
  originally) and history diverges
- could detect this lazily at chunk boundary, but node may have been
  serving incorrect queries

### Versioned eviction proposal

- Each metadata file acts as a manifest, it lists the explicit
  versions of all files it refers.
- Files, including the metadata file, has a version format like
  metadata-<first-row-id>_<effective-tx-id>.arrow
- Normally effective-tx-id is the last-tx-id of the chunk.
- When the transaction processing evicts, it writes a work item to the
  object store, with evict-<effective-tx-id>.arrow containing all
  row-ids to evict.
- Processing a work item happens in a background thread, occasionally
  waking up to see if there's any new work. It does its process in
  item order. It first checks if the work has already been done, by
  looking for a metadata file with the same effective-tx-id. If not,
  it downloads all columnar files, and removes the evicted row ids of
  the work item, and writes them under the new effective-row-id. It
  then writes the metadata file for this effective work id.
- When a new metadata file has been written for the same chunk that
  generated the effective-tx-id, the work item itself can be deleted.
- Nodes list all metadata files on startup, but only download the
  latest effective-tx-id per first-row-id.
- After a node has finished a chunk, it uploads it marking it with the
  last-tx-id, like usual.
- The node should preferably restart itself after each chunk, and pick
  up the latest versions of any metadata files. It can keep its buffer
  pool and id mapping, and fast forward the id map build up as an
  optimisation.
- Another thread occasionally wakes up and garbage collects all chunk
  versions older than N hours except the latest version. At this point
  the data has been evicted.
- If any node refers to an old metadata file and tries to download a
  deleted item it will fail and shutdown. This should only happen if a
  node falls really far behind.
- It's possible that old nodes may rewrite earlier and previously
  evicted chunks. This can partly be dealt with by sanity checking the
  latest manifests, which will be needed for restarting the processing
  anyway (as per above). This is still at risk from races, so in any
  case, these old chunks would be garbage collected anyway next time
  the eviction garbage collection happens.

### References

There are a few formats that try to achieve ACID semantics across a
single table on object stores.

- [Apache Iceberg](https://iceberg.apache.org/)
- [Apache Hudi](https://hudi.apache.org/)
- [Delta Lake](https://delta.io/)

They are all built around the idea of immutable files with some form
of manifest pointing to the set of files making up the latest
version. Updating this manifest requires a PUT If-Match style
operation to achieve the ACID semantics.

Of these, Delta Lake
([paper](https://databricks.com/wp-content/uploads/2020/08/p975-armbrust.pdf))
is most relevant for C2 eviction. They store a log of changes directly
in the object store and use the PUT If-Match to claim the latest
transaction id. The state of visible files itself is built up via
replaying the log, which also has regular checkpoints (and not via
single manifest files). When files are removed, they are later garbage
collected and deleted from the object store for real, which may trip
over clients reading at an old version. This entire scheme is similar
to the Versioned Eviction proposal above, except that we bypass the
need for PUT If-Match by managing the totally ordered log itself
elsewhere, like in Kafka.

Files in Delta Lake are smaller than the chunks we manage in C2, and
may only contain a few rows as the result of a single transaction. For
this reason they also suggest using compaction operations via the log
itself, which merges together a bunch of smaller files, removing them
and adding the combined result.

#### Write up of classic comparison (originally in ADR-0003)

Doesn't exist in c2 yet, but is somewhat orthogonal. Can be built in
various ways. A simple design I think may work is:

1. hard delete all overlap in the temporal index.
2. write row-id sets into the object store as a work items, named by
   the evict-ops row id.
3. out-of-band, pick all work-items you can find for a chunk, download
   it, copy it, skipping the row ids in the set.
4. upload it in place, breaking "immutable chunks" principle.
5. sanity check that the uploaded file is the one you expect, then
   delete the work items you processed, as this is done after, if
   someone else uploads before you, they would have seen the same or
   later work items. Might require some tweaks to actually be safe,
   but possible to design a protocol. The S3 database paper has one.
6. local caches will still contain the data, but it will be filtered
   out by the temporal index, let it disappear naturally from the
   nodes. If forced, cycle the nodes regularly to ensure this happens
   within regulatory time frames.

Note that eviction in c2 only deals with the object store, not the
transaction log, so if the log has infinite retention, one cannot
guarantee data being removed there. If this is a deal-breaker, we
could redesign the document topic with slightly different constraints
than now, but reopens many issues with eviction and complexity.

## Decision

We will implement the "versioned eviction proposal" above. We do not
want to introduce distributed locks or consensus for this use case.

## Consequences

It will be possible to bypass the immutable historical data XTDB
advocates when needed.

This complicates the persistence model which based on a shared,
eventually consistent object store.
