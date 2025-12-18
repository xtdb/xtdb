# Compaction

This document assumes understanding of the ["Building a Bitemporal Index"](https://xtdb.com/blog/building-a-bitemp-index-1-taxonomy) trilogy.

The consumer of this work is the scan operator - it reads the trie catalog to understand which files it has to read for a given query.

## Compaction strategy

We'll first discuss the strategy of how we choose which files to compact, implemented in the `xtdb.compactor` namespace.

This strategy has several required and desired properties:

* Nodes should be able to compact the LSM trie collaboratively, without coordination.
  
  Both the strategy and the implementation must therefore be deterministic - both choosing the same jobs regardless of the current state of the given node, and ensuring that the same output file contains the same data.
  Some nodes may be behind others in terms of what files they know to be available - even in this case, all of the possible intermediate states that the system may be in must be valid.
* We want to minimise re-work - the jobs should be of a fine-enough granularity that nodes will likely not choose the same job.
* We want files on the object store to be of similar sizes (~100MB), so that we don't have to handle some files being orders of magnitude larger than others.
* Files should, where possible, be entirely replaced by their compacted versions.

  That is, that once the output file has been written to the object store, the input files no longer need to be read.
  Particularly, we want to avoid the case where we need to figure out which _parts_ of a file have been compacted, as this adds complexity to the scan operator.

We split the LSM tree into two sides - 'current' ('C') and 'historical' ('H') - based on the recencies of their events: current files have recency = ∞, historical files have recency ≠ ∞.

The strategy has several stages:

1. **L0 files are compacted into L1H and L1C files**

   We do this immediately, as soon as there is an available L0 file.

   The inputs to this job are:
   * A partial L1C file, if it exists (i.e. one that is < 100MB)
   * The uploaded L0 file.

   The outputs are:
   * A new L1C file for events with recency = ∞, superseding the previous partial file.
   * 0..N L1H files for events with recency ≠ ∞, partitioned by the recency of the events into seven-day partitions, rolling over every Monday at 0000Z.

   At this stage, we partition the events by recency, on the assumption that new rows are more likely to be superseded quickly (the 'Lindy effect').

   In practice, what tends to happen:
   * For 'user profile' data, there's relatively little churn, so we don't expect to see many L1H files.
     The L1C file fills up, and gets compacted into L2C (stage 3).
   * For 'readings' data, where every event has a user-specified valid-to, the L1C never has any events.

   The L1H files are considered 'nascent' until the corresponding L1C file is uploaded - the L1C file serves as a marker that the compaction job is complete.
   At that point, the previous L1C file and the L0 file are considered 'garbage'.

2. **L1H files are compacted into L2H files.**
   Each recency partition is considered in isolation.

   The inputs to this job are:
   * A partial L2H file in that partition, if one exists.
   * Up to four (or up to three if there's a partial L2H file) L1H files in that partition.

   This happens either when there are four available files for a given recency partition, or when the total size of the partial L2H and the L1H files exceed 100MB.

   The output file is a new L2H which supersedes the previous partial file and the input L1H files.

3. **L1C files, when they are 'full' (> 100MB), are compacted into L2C files, partitioned by the first IID segment (two bits) of the events' IIDs.**

   The inputs to this job are four full L1C files.
   The output of this job is one L2C file containing one partition of the events.

   That is, there are four jobs created here, one for each partition.

   When the L2C file is uploaded, it is considered 'nascent' until all four partitions have been uploaded.
   Once the fourth file has been uploaded, the four L2C files are all marked live, and the four L1C are marked garbage.

   Similarly, four full L2H files are compacted into L3H files.

4. **Four L2C files in the same partition are compacted into an L3C file (etc).**

   L3Cs are partitioned by two IID segments (e.g. `p13`), L4Cs by three, etc.

   Same, but we don't need the 'is full' check here - we assume that the files will remain ~100MB all the way down.

   Similarly, four L3H files in the same partition are compacted into L4H files (etc).

   L4Hs are partitioned by two IID segments, L5Hs by three, etc.

## File name encoding

This information is encoded in the file names, e.g.:

- `l00-rc-b00` is level 0, current recency, block 0
- `l01-rc-b00` is level 1, current recency, block 0
- `l01-r20200106-b00` is level 1, historical recency, week ending 2020-01-06, block 0
- `l02-rc-p0-b00` is level 2, current recency, partition 0 (IIDs with first two bits `00`), block 0
- `l04-r20200106-p13-b00` is level 4, historical recency, week ending 2020-01-06, partition 13 (IIDs with first four bits `0111`), block 0.

Levels and blocks are encoded as 'lex hex' (lexicographical hexadecimal), so that they sort correctly - the first digit is the length of the hex string minus one, the remainder is the hex string.
`b134` is block `0x34` (decimal 52).

## Compaction algorithm

The algorithm is implemented in the `xtdb.compactor` Kotlin package.
Each compaction job yields one or more output segments; each output segment consists of a data and a meta file.

Writing the data file has two stages, writing intermediate results to disk between stages to minimise memory usage.

1. **Merge pages to disk**

   Similarly to the scan operator, we calculate a list of merge tasks. 
   For each merge task, we merge-sort the input pages by IID and system-time, applying bitemporal resolution.

   The behaviour here differs slightly depending on whether this is an L0 -> L1C/L1H compaction:

   - **L0 -> L1C/L1H**: The recency is calculated for each event through the bitemporal resolution.
     Events with recency = ∞ go to the current output file; events with a finite recency are routed to historical output files, partitioned into weekly buckets.

   - **Other compactions**: The recency from the input file's trie key is preserved into a single output file.

   At the end of each merge task (i.e. for each IID path prefix), the accumulated rows are flushed to a temporary file on disk.
   This bounds the memory usage of the compactor.

2. **Normalise page sizes**

   We then read the temporary files and write the final output files, normalising page sizes along the way.

   The input to this stage is a tree structure representing the pages written during the merge stage, along with their row counts.

   For each node in the tree:

   - **If a leaf has more rows than the page size**: it is split by re-partitioning the rows by IID.
     This recursively creates new branches until each leaf fits within the page size limit.

   - **If a branch has fewer rows than the page size**: all its descendant leaves are coalesced into a single page.

   This ensures output pages are consistently sized (~1024 rows), regardless of the distribution of rows in the input files.

We then construct the metadata file for this data file, and upload both to the object store.
The data file is uploaded first; the presence of the meta file is then considered the 'marker' that the job is completed.
We then submit a 'tries added' message to the log of the primary database.
