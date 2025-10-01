---
title: Storage
---

<details>
<summary>Changelog (last updated v2.1)</summary>

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

:::note
When using a remote storage implementation as part of a distributed cluster of XTDB nodes, we **must** ensure that all nodes are able to efficiently communicate the stream of file changes they make to the remote storage.
We achieve this inter-node messaging using a [**remote log**](log#Remote) implementation.

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

Each Object Store implementation is configured separately - see the individual cloud platform documentation for more information:

- [AWS](../aws#storage)
- [Azure](../azure#storage)
- [Google Cloud](../google-cloud#storage)
