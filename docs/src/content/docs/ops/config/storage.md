---
title: Storage
---

<details>
<summary>Changelog (last updated v2.3)</summary>

v2.3: disk-cache entries are keyed by the store they came from

: The disk cache keys its entries by the object store they were read from or written to — see [Disk cache](#disk-cache).

  Previously entries were keyed by database name, so a database re-attached under the same name on a different store could be served the previous store's cached files.

  No configuration changes are needed.
  Entries cached by earlier versions are not reused, so a disk cache that survives the upgrade starts cold once.
  If a warm cache matters, bring up upgraded nodes read-only on a fresh disk-cache directory, warm them with representative queries, then move traffic over.

v2.1: multi-database support

: As part of the multi-database support, the `memoryCache` and `diskCache` keys were extracted from the local/remote storage.

  Prior to that, the keys related to the `memoryCache` and `diskCache` were nested under the local/remote storage:

  ``` yaml
  storage: !Local
    path: /var/lib/xtdb/storage
    # maxCacheEntries: 1024
    # maxCacheBytes: 536870912

  # became

  storage: !Local
    path: /var/lib/xtdb/storage

  memoryCache:
    # maxSizeRatio: 0.5
    # maxSizeBytes: 536870912
  ```

  ``` yaml
  storage: !Remote
    objectStore: <ObjectStoreImplementation>
    localDiskCache: /var/lib/xtdb/remote-cache
    # maxCacheEntries: 1024
    # maxCacheBytes: 536870912
    # maxDiskCachePercentage: 75
    # maxDiskCacheBytes: 107374182400

  # became

  storage: !Remote
    objectStore: <ObjectStoreImplementation>

  diskCache:
    path: /var/lib/xtdb/remote-cache
    # maxSizeRatio: 0.75
    # maxSizeBytes: 107374182400

  memoryCache:
    # maxSizeRatio: 0.5
    # maxSizeBytes: 536870912
  ```
    
</details>

One of the key components of an XTDB node is the storage module - used to store the data and indexes that make up the database.

We offer the following implementations of the storage module:

- [In memory](#in-memory): transient in-memory storage.
- [Local disk](#local-disk): storage persisted to the local filesystem.
- [Remote](#remote): storage persisted remotely.

## In memory

By default, the storage module is configured to use transient, in-memory storage.

``` yaml
## default, no need to explicitly specify

## storage: !InMemory
```

## Local disk

A persistent storage implementation that writes to a local directory, also maintaining an in-memory cache of the working set.

``` yaml
storage: !Local
  # -- required

  # The path to the local directory to persist the data to.
  # (Can be set as an !Env value)
  path: /var/lib/xtdb/storage

### -- optional
## configuration for XTDB's in-memory cache
## if not provided, an in-memory cache will still be created, with the default size
memoryCache:
  # The maximum proportion of the JVM's direct-memory space to use for the in-memory cache (overridden by maxSizeBytes, if set).
  # maxSizeRatio: 0.5

  # The maximum number of bytes to store in the in-memory cache (unset by default).
  # maxSizeBytes: 536870912
```

## Remote

A persistent storage implementation that:

- Persists data remotely to a provided, cloud based object store.
- Maintains an local-disk cache and in-memory cache of the working set.

``` yaml
storage: !Remote
  # -- required

  # Configuration of the Object Store to use for remote storage
  # Each of these is configured separately - see below for more information.
  objectStore: <ObjectStoreImplementation>

### -- required
## Local directory to store the working-set cache in.
diskCache:
  ## -- required
  # (Can be set as an !Env value)
  path: /var/lib/xtdb/remote-cache

  ## -- optional
  # The maximum proportion of space to use on the filesystem for the diskCache directory (overridden by maxSizeBytes, if set).
  # maxSizeRatio: 0.75

  # The upper limit of bytes that can be stored within the diskCache directory (unset by default).
  # maxSizeBytes: 107374182400

### -- optional
## configuration for XTDB's in-memory cache
## if not provided, an in-memory cache will still be created, with the default size
memoryCache:
  # The maximum proportion of the JVM's direct-memory space to use for the in-memory cache (overridden by maxSizeBytes, if set).
  # maxSizeRatio: 0.5

  # The maximum number of bytes to store in the in-memory cache (unset by default).
  # maxSizeBytes: 536870912
```

### Disk cache

The disk cache holds copies of objects read from, or written to, the object store.

- Entries are keyed by the object store's location — its bucket or container, prefix and storage root — so databases attached to the same store share entries, and different stores never do.
- Entries leave the cache only when it needs the space, least-recently-used first: detaching a database doesn't remove its cached files, and a restarted node keeps the entries in its `diskCache.path`.
- Each node needs its own `diskCache.path`.
- If you reset a store in place — empty it and reuse the same location for a new database — clear the disk-cache directory of any node that keeps it across the reset, or use a new prefix.

Each Object Store implementation is configured separately - see the individual cloud platform documentation for more information:

- [AWS](../aws#storage)
- [Azure](../azure#storage)
- [Google Cloud](../google-cloud#storage)
