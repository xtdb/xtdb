## Eviction

### Deterministic Databases

Summary from James on Slack:

this sounds like a good point to take a step back and take a quick summary of where we're at - particularly, these two:

- we want to eventually evict content from the shared object store - it's non-trivial to get multiple nodes doing so in a way that both guarantees eventually evicting everything that needs to be evicted, but also without race conditions. impact of these race conditions could be that data gets unevicted:
  - say if chunk 1 has data evicted in chunks 3 and 5, we leave 'work items' for these
  - node A scrubs 3 and 5 and uploads;  - concurrently node B scrubs 3 and uploads - if we're in a last-write-wins world the data from 5 will remain.
  - PUT If-None-Match seems like a reasonable solution to this one for GCS and Azure Blobs; unfortunately S3 (while it is now read-after-write consistent) doesn't support it. there are workarounds involving SQS, but these add complexity.
  - the fact that we're deterministic/how far we go along the deterministic path doesn't affect this one?

- transactions replaying the log later need to arrive at the same conclusions about whether a transaction commits or not as a node playing through live - this means that we cannot evict any data before every node has indexed every transaction that depends on it.
  - we considered imposing bounds on how far a node could be behind, but realised it wouldn't be easy (possible?) to guarantee this bound - a node may not know how far it is behind until it comes to a chunk boundary (or whenever it checks, which could be more frequent but more frequent checking is more expensive), by which time it could have served inconsistent query responses.
 - this is where Hyder et al come in - they suggest keeping a read-set for the transaction (at various granularities), and determining commit/abort pessimistically based on whether anything in the read-set has changed at all in the 'conflict period' (between a transactions last-seen-tx-before-submission and the actual preceding tx).
 - would result in more aborted transactions than if we could guarantee our ability to re-do the read whenever the transaction got replayed.
 - Hyder et al don't have this problem (not being able to re-do the read) because each replica has a full snapshot of the data (?) - we have a shared object store.
 - importantly, we don't need to do anything about this right now, until we have either match or tx-fns - our put/delete transactions have empty read-sets.

### Versioned Eviction

- Each metadata file acts as a manifest, it lists the explicit versions of all files it refers.
- Files, including the metadata file, has a version format like metadata-<first-row-id>_<effective-tx-id>.arrow
- Normally effective-tx-id is the last-tx-id of the chunk.
- When the transaction processing evicts, it writes a work item to the object store, with evict-<effective-tx-id>.arrow containing all row-ids to evict.
- Processing a work item happens in a background thread, occasionally waking up to see if there's any new work. It does its process in item order. It first checks if the work has already been done, by looking for a metadata file with the same effective-tx-id. If not, it downloads all columnar files, and removes the evicted row ids of the work item, and writes them under the new effective-row-id. It then writes the metadata file for this effective work id.
- When a new metadata file has been written for the same chunk that generated the effective-tx-id, the work item itself can be deleted.
- Nodes list all metadata files on startup, but only download the latest effective-tx-id per first-row-id.
- After a node has finished a chunk, it uploads it marking it with the last-tx-id, like usual.
- The node should preferably restart itself after each chunk, and pick up the latest versions of any metadata files. It can keep its buffer pool and id mapping, and fast forward the id map build up as an optimisation.
- Another thread occasionally wakes up and garbage collects all chunk versions older than N hours except the latest version. At this point the data has been evicted.
- If any node refers to an old metadata file and tries to download a deleted item it will fail and shutdown. This should only happen if a node falls really far behind.
- It's possible that old nodes may rewrite earlier and previously evicted chunks. This can partly be dealt with by sanity checking the latest manifests, which will be needed for restarting the processing anyway (as per above). This is still at risk from races, so in any case, these old chunks would be garbage collected anyway next time the eviction garbage collection happens.
