# Memory: what a JVM in a Kubernetes container is charged for

A container's memory limit is enforced by the Linux kernel, which charges the container for every page its processes bring into memory — not only what the JVM reports.
This document assumes Java 21 or later, and cgroup v2; [The moving parts](#the-moving-parts) defines the Kubernetes and kernel terms used below.

- **Each page lands in one of four buckets, and only one of them can be given back.**
  Memory the JVM allocates itself (heap, direct buffers, metaspace, stacks) can't be given back; the kernel's cached copies of files can; in-memory filesystems and the kernel's own memory can't.

- **A page is charged to the cgroup that brought it in, until the page is freed.**
  Closing or unmapping a file doesn't move its pages out of your charge.

- **At the limit, the kernel reclaims first and OOM-kills second.**
  What gets a container killed is memory that can't be reclaimed.

- **Kubernetes watches a different number: the working set.**
  It evicts on that number, separately from the kernel, and active page cache counts towards it — so a container can be evicted for memory the kernel would have reclaimed.

- **The JVM's flags bound some regions and not others.**
  A pod's limit has to cover the bounded ones added up, plus everything no flag bounds.

- **Off-heap memory passes through several layers, and each counts only its own.**
  [The stack](#the-stack) lays them out; freeing at one layer doesn't mean the layer below has given the memory back.

## The moving parts

### The kernel

- **The kernel accounts memory in pages**, of 4 KiB on x86-64.

- **Anonymous memory is memory with no file behind it.**
  It's what `malloc` and anonymous `mmap` return, so it's where the JVM's heap and direct buffers live.

- **The page cache is the kernel's in-memory copy of file contents.**
  It keeps what a process read or wrote, so the next access to the same part of the file skips the disk.
  A memory-mapped file's pages are page-cache pages too.
  A cached page that's been modified and not yet written back to its file is dirty.

- **tmpfs is a filesystem whose files live only in memory.**
  Its pages look like page cache, but no disk file stands behind them.

- **Swap is disk space the kernel can move anonymous and tmpfs pages out to.**
  Without it, an anonymous page is freed only when its owner frees it, and a tmpfs page only when its file is deleted.

- **Reclaim is the kernel freeing memory by dropping pages it can get back later.**
  A clean page-cache page can be dropped and re-read from its file; a dirty one has to be written back first.
  The kernel keeps the page cache on two lists, active and inactive: recently re-used pages sit on the active list, and reclaim prefers the inactive one.

- **The OOM killer is the kernel's last resort when reclaim can't free enough**: it kills a process to get its memory back.

- **A cgroup (control group) is a set of processes the kernel accounts and limits together.**
  Every page those processes bring into memory is charged to their cgroup.
  - **`memory.max` is a cgroup's hard limit**: "Memory usage hard limit. This is the main mechanism to limit memory usage of a cgroup."
  - **`memory.current` is what the cgroup is using against that limit**: "The total amount of memory currently being used by the cgroup and its descendants."
  - **`memory.stat` breaks that usage down by kind, one field per kind.**
    This document names each kind by its field: `anon` is anonymous memory, `file` the page cache (tmpfs included), `shmem` tmpfs on its own, and `active_file` and `inactive_file` the page cache on the active and inactive lists.
    [Reading `memory.stat`](#reading-memorystat) lists them all.

- **On a cgroup v1 host the model holds, but the file names differ** (the limit is `memory.limit_in_bytes`); `stat -fc %T /sys/fs/cgroup/` prints `cgroup2fs` on v2.

### Kubernetes

- **A node is one machine, with one kernel; a pod is one or more containers that Kubernetes places on a node together.**
  The pod is the unit Kubernetes schedules and evicts.
  To the node's kernel, each container is a set of processes in a cgroup of its own: "the container runtime typically configures kernel cgroups that apply and enforce the limits you defined."

- **The kubelet is Kubernetes' agent on each node.**
  It starts a pod's containers and has their cgroups configured.
  It also watches the node's memory as a whole, and evicts pods when the node runs short — a mechanism of its own, separate from the kernel's OOM killer.

- **Each container declares a memory request and a memory limit.**
  - **The limit is enforced by the kernel, as a hard limit on the container's cgroup.**
    "Both `cpu` and `memory` limits are applied by the kubelet (and container runtime), and are ultimately enforced by the kernel."
    assumption: it's written as the cgroup's `memory.max`.
  - **The request is a reservation Kubernetes makes when placing the pod, not a kernel limit.**
    A pod is placed only on a node with room for its requests; "The kubelet also reserves at least the _request_ amount of that system resource specifically for that container to use."
    The container may use more than its request when the node has memory spare: "it's possible (and allowed) for a container to use more resource than its `request`".
    A limit with no request gets a request equal to the limit.

- **A pod's QoS class follows from its requests and limits, and decides which pods the kubelet evicts first.**
  - **`Guaranteed`**: every container's memory and CPU requests equal its limits.
    These pods "are guaranteed not to be killed until they exceed their limits".
  - **`Burstable`**: some request or limit is set, but the pod isn't `Guaranteed`.
    These "are evicted only after all `BestEffort` Pods are evicted."
  - **`BestEffort`**: no requests or limits at all.
    "The kubelet prefers to evict `BestEffort` Pods if the node comes under resource pressure."
  - **The class also decides who dies first if the whole node runs out of memory before the kubelet has evicted enough.**
    Then the kernel's OOM killer picks among every container on the node, and the kubelet weights its choice by class (`oom_score_adj`): `BestEffort` first, `Guaranteed` last.

- **An `emptyDir` is a scratch volume that lives as long as its pod does on that node.**
  - **By default it's a directory on the node's disk**: "stored on whatever medium that backs the node such as disk, SSD, or network storage".
    Files written there go through the page cache, charged to the container's cgroup like any other file I/O.
  - **With `medium: Memory` it's a tmpfs**, and "files you write count against the memory limit of the container that wrote them."

## What the cgroup charges: four buckets

**The cgroup charges anonymous memory, page cache, tmpfs and the kernel's own memory, and only clean page cache can be reclaimed without swap.**
The memory controller tracks "Userland memory - page cache and anonymous memory.", "Kernel data structures such as dentries and inodes." and "TCP socket buffers."

- **Anonymous memory (`anon`) can't be reclaimed without swap.**
  Everything the JVM allocates itself lands here: heap, direct buffers, metaspace, code cache, thread stacks, GC and JIT structures, `malloc`.
  assumption: Kubernetes nodes run without swap unless swap support has been configured for them.

- **Page cache (`file`) is reclaimable once it's clean.**
  Every file read or written through the kernel lands here, including memory-mapped files (`file_mapped`) and files written to a disk-backed `emptyDir`.
  - **A dirty page has to be written back before it can be dropped** (`file_dirty`, `file_writeback`).
  - **The JVM's own mapped files are page cache too** — its jars and its class-data-sharing archive.

- **tmpfs (`shmem`) is charged like page cache and can't be reclaimed without swap.**
  A memory-backed `emptyDir` (`medium: Memory`) is tmpfs: "The kubelet tracks `tmpfs` emptyDir volumes as container memory use", and "memory used by those files can not be reclaimed when the system or the pod are under memory pressure".
  - **`memory.stat` counts `shmem` inside `file`** — "including tmpfs and shared memory" — so `file` overstates what's reclaimable by `shmem`.

- **Kernel memory is charged too.**
  `kernel` is "Amount of total kernel memory, including (kernel_stack, pagetables, percpu, vmalloc, slab)"; socket buffers are counted separately, as `sock`.
  Of the slab, `slab_reclaimable` can be given back and `slab_unreclaimable` can't.

## Who a page is charged to

- **A page stays charged to the cgroup that instantiated it until the page is released.**
  The kernel's rule: "A memory area is charged to the cgroup which instantiated it and stays charged to the cgroup until the area is released."

- **So page cache outlives the file handle that brought it in.**
  Closing a file, or unmapping it, leaves its pages in the page cache and in your charge, until the kernel reclaims them.
  They don't move to a global, uncharged cache.

- **So a memory-mapped file costs the same as a file you read.**
  Its pages are page cache, counted as `file_mapped` while mapped and as plain `file` after, charged to the cgroup that faulted them in.

- **A page shared between cgroups is charged to one of them, not to each.**
  "To which cgroup the area will be charged is in-deterministic".

## At the limit: reclaim, then OOM-kill

**Enforcement is reactive: the kernel acts once usage reaches the limit.**
"A container may use more memory than its `memory` limit, but if it does, it may get killed."

1. **The kernel reclaims what it can** — clean page cache first, reclaimable slab.
2. **If usage can't be brought under the limit, the OOM killer runs inside the cgroup.**
   "If a cgroup's memory usage reaches this limit and can't be reduced, the OOM killer is invoked in the cgroup."
   The container shows as `OOMKilled`.

- **So what kills a container is memory that can't be reclaimed: anonymous memory, tmpfs, and the kernel's unreclaimable memory.**
  Page cache, however large, is what gets reclaimed on the way.

- **assumption: reclaim can still lose.**
  Dirty pages waiting on writeback, or anonymous memory growing faster than the kernel reclaims, can reach the OOM killer while reclaimable cache still exists.

## What Kubernetes measures: the working set

- **`Evicted` and `OOMKilled` are two different mechanisms, acting on two different numbers.**
  - **`OOMKilled` is the kernel**, on the cgroup's usage: at the container's own limit, or at the node's when the node runs out before the kubelet has evicted enough.
  - **`Evicted` is the kubelet**, on the working set, when the *node* runs short, ranking pods first by whether their usage exceeds their request.
    `BestEffort` and `Burstable` pods using more than they requested go first.

- **The kubelet's number is usage minus `inactive_file`.**
  It "excludes inactive_file (the number of bytes of file-backed memory on the inactive LRU list) from its calculation, as it assumes that memory is reclaimable under pressure."

- **Active page cache counts as used.**
  "The kubelet treats `active_file` memory areas as not reclaimable."
  A workload that reads or writes a lot of local files keeps many pages on the active list, and the kubelet "is liable to observe this as high resource use" and evict pods.

- **Active page cache can get a pod evicted, but never OOM-killed.**
  A page touched more than once is promoted to the active list, and reclaim takes from the inactive list first.
  When the node runs short, the kubelet ranks pods by their usage against their request, so a pod whose active page cache takes it over its request is among the first evicted — for memory the kernel would have reclaimed.
  At the container's own limit, by contrast, the kernel reclaims active page cache before it considers an OOM-kill.

- **Setting every container's requests equal to its limits (`Guaranteed`) keeps a pod out of eviction's first tier.**
  It's also Kubernetes' suggested workaround for the `active_file` issue: "setting the memory limit and memory request the same for containers likely to perform intensive I/O activity."

## The JVM's regions, and what bounds each

**Every region the JVM allocates itself is `anon`, and flags bound only some of them.**

| region | bucket | bounded by | default |
|---|---|---|---|
| heap | `anon` | `-Xmx` | 25% of the container limit (`MaxRAMPercentage`), with container support on |
| `java.nio` direct buffers | `anon` | `-XX:MaxDirectMemorySize` | `Runtime.maxMemory()`, roughly the heap maximum |
| Netty direct memory, on its no-`Cleaner` path ([Netty's direct memory](#nettys-direct-memory)) | `anon` | Netty's own counter: `io.netty.maxDirectMemory`, else the JVM's direct limit | off unless `io.netty.tryReflectionSetAccessible=true` and Netty has `Unsafe` and `java.nio` opened; otherwise Netty uses `java.nio` direct buffers, under both counters |
| FFM native memory: `Arena.ofConfined()`, `ofShared()`, `global()`, and `malloc` through the `Linker` | `anon` | nothing in the JVM — only whatever closes or frees it | — |
| FFM native memory: `Arena.ofAuto()` | `anon` | `-XX:MaxDirectMemorySize`, like `java.nio` direct buffers | — |
| metaspace | `anon` | `-XX:MaxMetaspaceSize` | unlimited |
| code cache | `anon` | `-XX:ReservedCodeCacheSize` | 240 MiB |
| thread stacks | `anon` | `-Xss` per thread; nothing bounds the number of threads | 1 MiB per thread (Linux/x64) |
| the JDK's per-thread temporary direct buffers, behind I/O on heap buffers | `anon` | `-Djdk.nio.maxCachedBufferSize` caps each one kept; nothing by default | unbounded |
| GC and JIT structures, symbol tables | `anon` | nothing | — |
| memory freed to `malloc` and kept by it | `anon` | nothing; the C library decides — see [The C allocator underneath](#the-c-allocator-underneath) | — |
| memory-mapped files | `file` | nothing in the JVM | — |
| page cache from file I/O | `file` | nothing in the JVM | — |

- **The heap is charged as it's touched, unless `-XX:+AlwaysPreTouch` touches it all up front.**
  "By default, this option is disabled and all pages are committed as the application uses the heap space."
  - **assumption: touched heap stays charged until the collector uncommits it**, which G1 does only after a full or concurrent cycle, or periodically where `G1PeriodicGCInterval` is set (JEP 346) — so the heap's charge ratchets towards `-Xmx`, not towards live data.

- **`MaxDirectMemorySize` covers `java.nio` direct buffers, not off-heap memory in general.**
  "Sets the maximum total size (in bytes) of the `java.nio` package, direct-buffer allocations."

- **Netty can hold a second allowance of direct memory, and Arrow allocates through Netty** — see [Netty's direct memory](#nettys-direct-memory) and [How Arrow allocates](#how-arrow-allocates).

- **Native Memory Tracking accounts the JVM's own allocations, by subsystem.**
  `-XX:NativeMemoryTracking=summary` (off by default), then `jcmd <pid> VM.native_memory summary`.
  `Unsafe.allocateMemory` — behind `java.nio` direct buffers and Netty's no-`Cleaner` buffers — shows under "Other" (`mtOther`); what it misses is `malloc` from native libraries, and memory `malloc` keeps after a `free`.

## The stack

**Off-heap memory passes down through layers, each getting its memory from the one below.**
Read top-down: an Arrow buffer is a piece of a Netty chunk, which is `malloc`'d memory, which is pages the kernel mapped and charged to the cgroup.
The rows link to the sections that follow, which cover each layer in turn.

| layer | gets memory from | gives it back by | counted by | zeroes |
|---|---|---|---|---|
| your application | Arrow allocators, the Java heap, mapped files | closing what it holds | its own budgets, if it keeps any | — |
| [Arrow allocators](#how-arrow-allocates) | Netty: a piece of a pooled 4 MiB chunk, or a buffer of its own above that; above 2 GiB (`Integer.MAX_VALUE`), `Unsafe` directly; or memory allocated elsewhere, such as a mapped file | releasing the buffer | each allocator's limit, root and children | a vector's `allocateNew` does; a raw `ArrowBuf` doesn't |
| [Netty's pool](#nettys-direct-memory) | `Unsafe.allocateMemory` or `allocateDirect`, 4 MiB at a time | freeing a chunk once it's emptied | Netty's own counter | no — a reused buffer holds what it last held |
| [`ByteBuffer.allocateDirect`](#nettys-direct-memory) | `Unsafe.allocateMemory` | its `Cleaner`, run by the GC or by Netty | `MaxDirectMemorySize` | yes, every byte |
| [FFM `Arena.allocate`](#ffm-memory) | `Unsafe.allocateMemory` | closing the arena | `MaxDirectMemorySize`, for GC-managed arenas only | yes, every byte |
| `Unsafe.allocateMemory` | `malloc` | `freeMemory` → `free` | nothing (Native Memory Tracking, under "Other") | no |
| [`FileChannel.map`](#loading-a-file-into-memory) | the page cache, mapped straight in | unmapping | nothing in the JVM | — (filled from the file) |
| Java heap, metaspace, code cache | the kernel, mapped by HotSpot directly — not through `malloc` | the GC handing unused heap back | `-Xmx`, `MaxMetaspaceSize`, `ReservedCodeCacheSize` | the heap: each new object |
| [the C library (`malloc`)](#the-c-allocator-underneath) | the kernel (`mmap`) | unmapping, or trimming (handing freed pages back) — or not at all | nothing | no |
| the kernel | physical memory | freeing the page | the cgroup: `memory.current` against `memory.max` | every fresh anonymous page |

- **Each counter drops when its own layer frees, not when the kernel does.**
  So Netty, the JDK and Arrow can all read low while `anon` reads high — see [The C allocator underneath](#the-c-allocator-underneath).
- **Nothing checks that the counters fit under the limit.**
  Each is set on its own; [Sizing a pod](#sizing-a-pod) adds them up.
- **Three routes skip `malloc`**: the Java heap (with metaspace and the code cache), which HotSpot maps itself; mapped files, which are page cache; and thread stacks, which the C library maps when it creates the thread.

**The kernel zeroes every anonymous page the first time it's written, and that write is when the page is charged.**
`mmap(2)`: an anonymous mapping's "contents are initialized to zero".
- **A first read charges nothing**: the kernel maps a shared page of zeros, and allocates a page of the process's own only on the first write.
  That's why untouched memory from `Unsafe.allocateMemory` isn't charged.
- **Trimming puts a page back in that state**: after `MADV_DONTNEED`, the next write faults and is zeroed and charged again.
- **A layer that zeroes on top writes every page, so the whole allocation is charged at once.**
  `allocateDirect` and `Arena.allocate` always do, and a new heap array is zeroed too; Arrow's fixed-width vectors zero their validity and value buffers on `allocateNew`, its variable-width vectors their validity and offset buffers.
- **It also pays a second write over the kernel's**, which is most of `allocateDirect`'s cost — see [Netty's direct memory](#nettys-direct-memory).

## Netty's direct memory

**Netty allocates direct memory in one of two ways, and they differ in who counts it and what it costs.**
As long as Netty has `sun.misc.Unsafe`, it frees explicitly either way: neither waits for a GC.

- **The JDK's way: `ByteBuffer.allocateDirect`, counted against `MaxDirectMemorySize`.**
  `allocateDirect` reserves the size against `MaxDirectMemorySize` (`Bits.reserveMemory`), allocates it with `Unsafe.allocateMemory`, zero-fills it, and registers a `Cleaner` whose `Deallocator` frees the memory and un-reserves it.
  - **Left to itself, the `Cleaner` runs when a GC finds the buffer unreachable**, so the native memory lives as long as a heap object.
  - **Netty doesn't leave it: it runs the `Cleaner` itself when it frees the buffer** (`Unsafe.invokeCleaner`).
    The JDK's count drops as soon as Netty releases the memory.

- **Netty's no-`Cleaner` way: Netty allocates the memory itself, and the JDK never sees it.**
  `Unsafe.allocateMemory`, with the address wrapped in a `DirectByteBuffer` through a private constructor that sets no `Cleaner` and reserves nothing; Netty frees it with `Unsafe.freeMemory`.
  - **Netty only takes it when four things hold**, and otherwise takes the JDK's way:
    - `-Dio.netty.tryReflectionSetAccessible=true` is set, since reaching the constructor takes reflection;
    - `java.nio` is opened to Netty (`--add-opens=java.base/java.nio=ALL-UNNAMED`), for the same reason;
    - Netty has `sun.misc.Unsafe` - see the JDK 25 bullet below;
    - `io.netty.maxDirectMemory` isn't `0`, which forces the JDK's way and turns Netty's counter off.

- **Netty also counts what it allocates in a counter of its own, either way.**
  It is capped at `io.netty.maxDirectMemory`, or the JVM's direct limit when that's unset, and going past it throws Netty's `OutOfDirectMemoryError`.
  - **On the no-`Cleaner` way it's the only count**, so the JDK's counter and Netty's each allow the full limit, and together they can reach twice `MaxDirectMemorySize`.
  - **On the JDK's way it counts after the JDK has reserved**, and only a subset of what the JDK counts, so the JDK's limit is reached first.
  - Measured (Netty 4.2.19 with `-Dio.netty.noUnsafe=false`, on JDK 21 and 25), one 1 MiB allocation: with `tryReflectionSetAccessible`, Netty's counter 1 MiB and the JDK's 0; without it, both 1 MiB.

- **The JDK's way costs a zero-fill, and the zero-fill makes the whole buffer resident at once.**
  - **Allocating and freeing is one to three orders of magnitude slower.**
    Measured on JDK 21 / 25: 4 MiB in 2.2 / 3.8 µs the no-`Cleaner` way, 69 / 77 µs the JDK's; 64 MiB in 9.7 / 9.0 µs, against 9.1 / 9.3 ms.
  - **Untouched memory from `Unsafe.allocateMemory` isn't charged; an untouched direct buffer is charged in full.**
    Measured: 256 MiB took `RssAnon` up by 0 MiB the first way and 256 MiB the second, on both JDKs.

- **On JDK 25+, Netty 4.2 avoids `sun.misc.Unsafe` unless `-Dio.netty.noUnsafe=false` or `--sun-misc-unsafe-memory-access=allow` is set.**
  Any other value of `--sun-misc-unsafe-memory-access` turns it off on every JDK.
  - **It falls back to FFM instead** ([FFM memory](#ffm-memory)): `malloc` through the `Linker` where it has native access, else one `Arena.ofShared()` per allocation.
    Neither is bounded by `MaxDirectMemorySize`; measured on JDK 25 with no Netty flags, the JDK's direct count stays at 0.
  - **An unpooled buffer on the `Arena` path can be left to the GC**: closing a shared arena is expensive, so where a caller doesn't permit that, Netty takes a plain `allocateDirect` it never frees, outside its own counter.
  - **Arrow can't run on these paths**: its allocator needs `PooledUnsafeDirectByteBuf`, which Netty only makes with `Unsafe`, and fails with "Arrow requires that the JVM used supports access sun.misc.Unsafe".
  - **The JDK warns on the first `sun.misc.Unsafe` memory access from JDK 24**; `--sun-misc-unsafe-memory-access=allow` silences it, and JDK 21 and 22 refuse that option and don't start.

**Netty pools: it allocates memory in chunks and carves buffers out of them.**
- **A pooled direct arena allocates a whole chunk at a time**, through whichever way above is in use, and hands out pieces of it.
  Netty calls its sub-pools arenas; they're unrelated to FFM's `Arena` and to glibc's arenas.
  - A chunk is `io.netty.allocator.pageSize` (8 KiB) shifted by `io.netty.allocator.maxOrder` (9): 4 MiB.
  - There are min(2 × cores, direct limit / chunk / 6) arenas (`io.netty.allocator.numDirectArenas`); each thread is bound to the one with the fewest threads, and an arena adds a chunk when its existing ones can't fit a request.
- **A released buffer goes back to the arena, or to a per-thread cache.**
  By default only Netty's own threads (`FastThreadLocalThread`s and event loops) get a cache; other threads' buffers go straight back to the arena.
- **A chunk's memory is freed only when it empties after having been at least 25% used.**
  A chunk that has never reached 25% stays however empty it gets, and one live buffer keeps a whole chunk.
- **A buffer larger than a chunk is allocated on its own, outside the pool, and freed when it's released.**
- **So the process holds more than the buffers in use**: memory the application has released stays allocated in the pool until Netty gives the chunk up.

## How Arrow allocates

**An Arrow allocator is a ledger; the memory comes from its allocation manager.**
`RootAllocator` and its child allocators count the bytes they hand out against their limits.
With `arrow-memory-netty`, `NettyAllocationManager` does the allocating, from one JVM-wide pooled Netty allocator (`PooledByteBufAllocatorL`).

**It takes one of three routes, by size.**
- **Up to Netty's chunk size: a piece of a chunk from Netty's pooled direct arena.**
- **Above a chunk, up to 2 GiB (`Integer.MAX_VALUE`): a Netty direct buffer of its own, outside the pool**, freed when Arrow releases it.
- **Above 2 GiB: `PlatformDependent.allocateMemory`, straight from `Unsafe`.**
  Neither Netty's counter nor the JDK's sees these.

**So Arrow's limits bound what Arrow handed out, not what the process holds for it.**
Netty holds whole chunks and keeps released space pooled, and the largest allocations escape every counter but Arrow's.

**A foreign allocation is counted by Arrow alone.**
`wrapForeignAllocation` makes an Arrow buffer over memory someone else allocated — a memory-mapped file, a native segment.
The Arrow allocator counts it against its limit, Netty's counter doesn't move because Netty never allocated it, and the kernel charges it to whichever bucket it really lives in: `file` for a mapped file, `anon` for native memory.

## FFM memory

**FFM (`java.lang.foreign`, final from JDK 22) gives Java off-heap memory with an explicit lifetime, and the JVM bounds only one kind of it.**

- **An `Arena` decides who may use its memory, and when it's freed.**
  - **`ofConfined()`**: one thread may use it, and closing it frees the memory.
  - **`ofShared()`**: any thread may use it, and closing it frees the memory.
    Closing has to make sure no other thread is mid-access, which makes it expensive to do often.
  - **`global()`**: never freed.
  - **`ofAuto()`**: freed when the GC finds it unreachable, like a `java.nio` direct buffer.

- **Only `ofAuto()` is reserved against `MaxDirectMemorySize`.**
  `ArenaImpl` reserves only for the implicit (GC-managed) session; the others are bounded by whoever closes them, and nothing in the JVM.
  Measured on JDK 25 under `-XX:MaxDirectMemorySize=64m`: 256 MiB from each of `ofConfined()`, `ofShared()` and `global()` succeeded; from `ofAuto()` it threw "Cannot reserve 268435456 bytes of direct buffer memory".

- **Every `Arena.allocate` zero-fills** (the javadoc: segments "are zero-initialized"), so it's charged in full at once — see [The stack](#the-stack).
  Measured: each 256 MiB `Arena` allocation took `RssAnon` up by 256 MiB.

- **Memory that's never zeroed comes from `malloc` through the `Linker`.**
  Call `malloc` as a downcall, and wrap the address with `MemorySegment.ofAddress(…).reinterpret(size, arena, cleanup)`, where the cleanup calls `free`.
  - **It's charged only as it's written, and nothing in the JVM bounds it.**
  - **`reinterpret` is a restricted method**: it needs `--enable-native-access` (`ALL-UNNAMED` on the classpath), or the JDK warns.

## The C allocator underneath

**Most off-heap memory reaches the kernel through the C library's `malloc`, and what `malloc` keeps after a `free` is still charged to `anon`.**
`Unsafe.allocateMemory` and `freeMemory` are `malloc` and `free` (HotSpot's `os::malloc`), so this covers `java.nio` direct buffers, Netty's chunks and unpooled buffers, the JDK's temporary I/O buffers, FFM `Arena.allocate`, and `malloc` through the `Linker`.
No JVM counter sees the difference: Netty's, the JDK's and Arrow's all drop at the `free`, while the cgroup's `anon` doesn't.

**Which C library it is depends on the image, and the two behave differently.**
Debian- and Ubuntu-based images (`eclipse-temurin:25`) use glibc; Alpine-based images (`eclipse-temurin:25-jre-alpine`, or a JRE built with `jlink` onto an `alpine` base) use musl.
`ldd --version`, or `ls /lib/ld-musl*` inside the container, tells you which.

**A large block that gets a mapping of its own is the easy case.**
The allocator asks the kernel for fresh anonymous memory (`mmap`); like any anonymous memory it's charged page by page as it's first written, and `free` hands the whole mapping back (`munmap`), which uncharges it at once.
Smaller blocks are carved out of bigger regions the allocator keeps, and that's where freed memory can stay charged.

**glibc keeps freed memory in arenas, and of its own accord hands back only some of it.**
- **An arena is a pool of memory regions with a lock of its own** — unrelated to Netty's arenas and FFM's `Arena`.
  glibc spreads threads across several, so they don't all queue on one lock.
  On 64-bit, glibc up to 2.43 allows 8 × CPUs arenas, and 2.44 allows one per CPU (at least 8).
  assumption: "CPUs" is what the host has online, not the container's CPU limit — glibc counts them with `__get_nprocs()`.
- **On `free`, an arena gives memory back only from the end of a region.**
  Once the free space at the end passes `M_TRIM_THRESHOLD` (128 KiB to start), glibc returns it; a free block below a live one stays charged until a later `malloc` reuses it.
- **`malloc_trim` hands back free pages anywhere in every arena**, and HotSpot calls it on request.
  `-XX:TrimNativeHeapInterval=<ms>` (off by default) trims periodically, and `jcmd <pid> System.trim_native_heap` trims once; on musl both do nothing.
- **Large blocks bypass the arenas, but "large" moves up.**
  A request of at least `M_MMAP_THRESHOLD` (128 KiB to start) gets its own mapping.
  But freeing such a block raises the threshold to that block's size, up to 32 MiB on 64-bit, and the trim threshold to twice that — so after Netty frees its first 4 MiB chunk, later 4 MiB chunks come from the arenas.
- **Setting either threshold by hand turns that adjustment off**, as does setting `M_TOP_PAD` or `M_MMAP_MAX` (`mallopt(3)`).

**musl gives every block of 131,052 bytes or more a mapping of its own.**
So a large block is never left behind; the cost is a fresh mapping, and a page fault per page as it's first written, on every allocation.

**glibc's knobs are environment variables, read before the first `malloc`.**
`MALLOC_ARENA_MAX`, `MALLOC_MMAP_THRESHOLD_` and `MALLOC_TRIM_THRESHOLD_` (note the trailing underscores) set the matching `mallopt` parameters; musl ignores them all.

**Netty's pool sits in front of all this, so its churn is in 4 MiB chunks, not buffers.**
What reaches `malloc` in mixed sizes is everything outside the pool — Arrow's buffers larger than a chunk, unpooled Netty buffers, `java.nio` direct buffers.

**Measured: freed memory stays charged on glibc, unless the threshold is pinned or the heap trimmed.**
32 threads each allocate, write and free 400 blocks through `Unsafe.allocateMemory`, holding 8 live at a time (9 for a moment, before each free) — about 1.13 GiB live at most — then free everything.
JDK 25, glibc 2.44 on 16 CPUs, musl from Alpine 3.24.4; two runs each, each in a cgroup of its own so that `anon` is the probe's alone.

| blocks | allocator | peak `anon` | `anon` once all freed | time |
|---|---|---|---|---|
| 64 KiB–8 MiB | glibc, defaults | 1.77–1.82 GiB | 0.88–1.03 GiB | 0.8–1.1 s |
| 64 KiB–8 MiB | glibc, `MALLOC_ARENA_MAX=2` | 1.49–1.56 GiB | 1.47–1.48 GiB | 0.7–1.2 s |
| 64 KiB–8 MiB | glibc, `MALLOC_MMAP_THRESHOLD_=131072` | 1.21–1.25 GiB | 45–46 MiB | 4.2 s |
| 64 KiB–8 MiB | glibc, `-XX:TrimNativeHeapInterval=100` | 1.74–1.80 GiB | 102–179 MiB, 1 s later | 1.2–1.3 s |
| 64 KiB–8 MiB | glibc, `-XX:TrimNativeHeapInterval=1000` | 1.74–1.83 GiB | 78–139 MiB, 1 s later | 1.1–1.2 s |
| 64 KiB–8 MiB | glibc, `MALLOC_TRIM_THRESHOLD_=131072` | 1.25–1.27 GiB | 44–56 MiB | 4.1 s |
| 64 KiB–8 MiB | musl | 1.22–1.24 GiB | 31–32 MiB | 4.5 s |
| 4 MiB | glibc, defaults | 1.18 GiB | 0.87–0.91 GiB | 1.1 s |
| 4 MiB | glibc, `MALLOC_ARENA_MAX=2` | 1.18–1.19 GiB | 0.70–0.78 GiB | 0.8–1.2 s |
| 4 MiB | glibc, `MALLOC_MMAP_THRESHOLD_=131072` | 1.15–1.17 GiB | 45 MiB | 5.3–5.4 s |
| 4 MiB | musl | 1.14 GiB | 30 MiB | 4.7–4.8 s |

- **Mixed sizes are what inflate glibc's peak**: about 60% over the live data by default, about 35% with two arenas.
  Same-sized blocks reuse each other's space, so the peak stays within about 5% of the live data.
- **`MALLOC_ARENA_MAX` lowers the peak, but not reliably what's kept**: on mixed sizes two arenas kept half as much again as the default, on same-sized blocks somewhat less.
- **Trimming hands back what's kept, and leaves the peak alone.**
- **Setting `MALLOC_TRIM_THRESHOLD_` behaves like pinning the mmap threshold, not like trimming.**
  It only changes how much free space at the end of a region `free` leaves; its effect here is the side effect — any threshold set by hand freezes the mmap threshold at 128 KiB.
- **Mapping every large block — a pinned threshold, or musl — keeps the peak within about 10% of the live data and gives back nearly everything, at 3.6–5.6× the time on this churn.**
  The time goes in the kernel, on fresh pages: a pinned threshold took 5.75 million page faults and 24 s of system CPU across the threads, against 0.35 million and 1.8 s with defaults — every new mapping is faulted in and zeroed page by page, which a reused arena block isn't.
  Trimming costs some of the same: trimmed pages fault back in when they're reused, nearly doubling the faults at a 100 ms interval, with no measurable change in wall time here.

## Loading a file into memory

**How a JVM reads a file decides how many copies it holds, which bucket each lands in, and when each is charged.**
Each row was measured on JDK 25: a 256 MiB file, evicted from the page cache first, loaded in a cgroup of its own, every page then read; the figures are the cgroup's `memory.stat` deltas, in MiB.

| method | copies | page cache (`file`) | your copy | your copy charged | JVM bound on your copy | after you release it |
|---|---|---|---|---|---|---|
| `Files.readAllBytes`, or `FileChannel.read` into a heap `ByteBuffer` | 3 | +256 | heap `anon` +256, plus a temporary direct buffer `anon` +256 | the heap array when allocated (zeroed); the temporary buffer as read | `-Xmx`; the temporary buffer by nothing | heap stays committed; the temporary buffer stays cached on the thread; page cache stays |
| `FileChannel.read` into `ByteBuffer.allocateDirect` | 2 | +256 | `anon` +256 | at allocation (zero-filled) | `MaxDirectMemorySize` | freed when the GC runs the `Cleaner`, or the owner does; page cache stays |
| `FileChannel.read` into `Unsafe.allocateMemory` (Netty's no-`Cleaner` buffers) | 2 | +256 | `anon` +256 | as the read writes it | nothing in the JVM — Netty's or Arrow's own counters | freed explicitly; page cache stays |
| `FileChannel.read` into `Arena.ofShared().allocate` | 2 | +256 | `anon` +256 | at allocation (zero-filled) | nothing (`ofAuto()`: `MaxDirectMemorySize`) | freed when the arena closes; page cache stays |
| `FileChannel.map` (`MappedByteBuffer`) | 1 | +256, all of it `file_mapped` | — the mapping is the page cache | as each page is first touched | nothing | unmapped when the GC collects the buffer; `file_mapped` → 0, `file` stays |
| `FileChannel.map(…, arena)` (`MemorySegment`) | 1 | +256, all of it `file_mapped` | — the mapping is the page cache | as each page is first touched | nothing | unmapped when the arena closes; `file_mapped` → 0, `file` stays |
| `O_DIRECT` (`ExtendedOpenOption.DIRECT`) into an aligned direct buffer | 1 | 0 | `anon` +256 | at allocation (zero-filled) | `MaxDirectMemorySize` | freed as `allocateDirect` above; nothing left behind |

- **Every buffered read leaves a page-cache copy charged to you after you've let go of your own.**
  It's reclaimable, and counts towards the working set while it's active — see [Who a page is charged to](#who-a-page-is-charged-to).

- **A read into a heap buffer goes through a temporary direct buffer the same size, and the thread keeps it.**
  The JDK copies through a per-thread cache of direct buffers (`sun.nio.ch.Util.getTemporaryDirectBuffer`), allocated with `Unsafe.allocateMemory`, so `MaxDirectMemorySize` doesn't count them.
  The cache keeps buffers of any size unless `-Djdk.nio.maxCachedBufferSize` caps it: measured, capping it at 256 KiB took both heap methods from `anon` +500 to +250.

- **Mapping holds one copy, but a mapped page counts as both page cache and memory in use.**
  `file_mapped` pages are reclaimable; assumption: the ones being touched are promoted to the active list, and so count in the working set.
  Unmapping doesn't uncharge them — they stay in `file` until reclaimed.

- **`O_DIRECT` is the only route that leaves no page cache behind.**
  The buffer, the file offset and the length must be aligned to the device's block size; `alignedSlice` does the buffer.

- **"Charged at allocation" is the zero-fill** — see [The stack](#the-stack).

### Which route other databases take

**Databases split three ways: their own cache over the page cache, `O_DIRECT` into their own cache, or the page cache alone.**
Each is the default unless the row says otherwise; checked against each project's docs or source, 2026-11-10.

| database | route by default | the switch, and its default | what bounds its own memory | cgroup-aware? |
|---|---|---|---|---|
| PostgreSQL 18 | own cache over the page cache: buffered reads into `shared_buffers` | `debug_io_direct`, off, "intended for developer testing only" | `shared_buffers`, 128 MiB | not documented |
| MySQL 8.4 / InnoDB | `O_DIRECT` into the buffer pool | `innodb_flush_method`, `O_DIRECT` on Unix where supported | `innodb_buffer_pool_size`, 128 MiB; `innodb_dedicated_server` sizes it from detected memory | not documented |
| DuckDB | own cache over the page cache: `pread` into its buffer manager | none user-facing | `memory_limit`, 80% of the smaller of RAM and the cgroup limit | yes |
| ClickHouse | own caches over the page cache: `pread`, via a thread pool on a miss | `min_bytes_to_use_direct_io` and `min_bytes_to_use_mmap_io`, both 0; `local_filesystem_read_method` | `max_server_memory_usage_to_ram_ratio`, 0.9 | yes |
| ScyllaDB (Seastar) | `O_DIRECT` into its own cache | `--kernel-page-cache`, off | the smaller of RAM and the cgroup limit, less a reserve of max(1.5 GiB, 7%) | yes |
| Apache Cassandra 5.0 | mmap for index files; buffered reads for data files | `disk_access_mode`, `mmap_index_only` (4.1: `auto`, which maps both) | the JVM heap; its chunk cache is off by default | — |
| RocksDB | own cache over the page cache: buffered reads into the block cache | `use_direct_reads` and `allow_mmap_reads`, both off | block cache, 32 MiB | unverified |
| MongoDB / WiredTiger | own cache over the page cache, deliberately: compressed in the page cache, uncompressed in its own | none | max(50% of (RAM − 1 GB), 256 MB); containers "must set `--wiredTigerCacheSizeGB`" | partly |
| SQLite | own cache over the page cache: `xRead` into a page buffer | `PRAGMA mmap_size`, 0 | `PRAGMA cache_size`, about 2 MiB | — |
| Elasticsearch / Lucene | mostly the page cache: mmap for term dictionaries, norms and doc values, buffered reads for the rest | `index.store.type`, `hybridfs` | the JVM heap; "at least half the available memory goes to the filesystem cache" | heap only |
| Apache Kafka | the page cache alone: buffered reads, `sendfile` to consumers | none | the JVM heap; an in-process cache "will likely be duplicated in OS pagecache, effectively storing everything twice" | — |
| QuestDB | the page cache alone: mmap of column files | none | `ram.usage.limit.percent`, 90 — which excludes mapped files, so it "does not stop the kernel from killing the process" | yes |
| LMDB | the page cache alone: mmap, reads return pointers into the map | `MDB_WRITEMAP` for writes | none of its own | — |
| Neo4j | own cache over the page cache: its off-heap page cache (read mechanism unverified) | `server.memory.pagecache.directio`, off, Linux and some record formats only | `server.memory.pagecache.size`: 50% of RAM less the heap, by default | — |

- **Caching twice is often deliberate.**
  PostgreSQL "relies on the operating system cache", and WiredTiger keeps compressed pages in the page cache and uncompressed ones in its own.
- **`O_DIRECT` is the route of the databases that own their memory outright** — InnoDB "to avoid double buffering", Seastar by default.
- **The page-cache-only route leaves a database's biggest memory user outside any limit of its own**, which QuestDB's docs spell out.

## Sizing a pod

**The limit has to cover what can't be reclaimed, added up.**

- `-Xmx`
- `+` direct memory, once per counter in use — the JDK's, and Netty's where its no-`Cleaner` path is on
- `+` `MaxMetaspaceSize` — set it, or metaspace has no bound
- `+` `ReservedCodeCacheSize`
- `+` threads × `-Xss`
- `+` the JDK's temporary direct buffers — one per thread doing I/O on heap buffers, as large as its largest read, unless `-Djdk.nio.maxCachedBufferSize` caps them
- `+` off-heap memory your own code bounds — FFM arenas other than `ofAuto()`, `malloc` through the `Linker`
- `+` native overhead no flag bounds — measured, with Native Memory Tracking
- `+` what `malloc` holds beyond the live off-heap memory — on glibc it depends on the size mix, the thread count and the host's CPUs, so measure it; one synthetic churn saw 60% on top at the peak
- `+` memory-backed `emptyDir` volumes
- `+` the kernel's own memory charged to the cgroup — `kernel` and `sock` in `memory.stat`, measured

**Page cache doesn't have to fit, but it decides how much of the remainder is useful.**
- **What's left under the limit is where the page cache lives**, and the kernel reclaims it to make room for anything above.
- **Its active part counts towards the working set**, so a pod whose request is below its limit can be evicted for it.

**The JVM's defaults alone allow most of the limit before anything else.**
These are allowances, not up-front charges: the memory is charged as it's touched.
- **Unflagged, the heap gets 25% of the limit, and `java.nio` direct memory up to the same again** — 50%.
- **With Netty's no-`Cleaner` path on, Netty's own allowance adds another 25%** — 75%, before metaspace (unbounded), code cache, stacks and native overhead.

**Hand-written flags replace the container-derived heap size, and nothing checks that they fit.**
- **`-Xmx` replaces only the heap size the JVM derives from the container**; container detection still sizes the processor count.
- **With `-Xmx` and `MaxDirectMemorySize` set by hand, their sum is never compared with the limit** — a mismatch shows up as an `OOMKilled`.
  DuckDB and ClickHouse derive their budgets from the cgroup's limit instead — see [Which route other databases take](#which-route-other-databases-take).

## Reading `memory.stat`

**`memory.stat` is the cgroup's own account, and the only one the limit is enforced against.**
From inside the container, the cgroup's files are under `/sys/fs/cgroup/`.
assumption: the container runtime mounts the container's own cgroup there, as cgroup namespaces do by default.

| file / field | what it is |
|---|---|
| `memory.current` | "The total amount of memory currently being used by the cgroup and its descendants." |
| `memory.max` | the hard limit |
| `anon` | "Amount of memory used in anonymous mappings such as …" — the JVM's own allocations |
| `file` | "Amount of memory used to cache filesystem data, including tmpfs and shared memory." |
| `file_mapped` | "Amount of cached filesystem data mapped with mmap()." |
| `file_dirty`, `file_writeback` | page cache waiting for, or undergoing, writeback |
| `shmem` | "Amount of cached filesystem data that is swap-backed, such as tmpfs, shm segments, …" |
| `active_file`, `inactive_file` | page cache on the reclaim algorithm's two lists; the kubelet subtracts only `inactive_file` |
| `kernel` | "Amount of total kernel memory, including (kernel_stack, pagetables, percpu, vmalloc, slab)" |
| `sock` | "Amount of memory used in network transmission buffers" |

- **Unreclaimable usage is roughly `anon + shmem + kernel + sock − slab_reclaimable`.**
  That's the part the limit has to hold.
- **The working set is `memory.current − inactive_file`.**
  That's the part Kubernetes compares with the request.
- **`anon` well above the JVM's own counters is native overhead, native libraries' `malloc`, or memory `malloc` has kept.**
  Native Memory Tracking accounts only the first, and counts what's committed rather than what's touched, so its total isn't a baseline to subtract.
  On glibc, `jcmd <pid> System.trim_native_heap` tells you how much `malloc` was keeping: it prints the process's RSS+Swap before and after (`Trim native heap: RSS+Swap: 114M->114M (-4096B)`), and the drop is what `malloc` was keeping.

## Sources

- [Linux kernel: Control Group v2 — Memory](https://docs.kernel.org/admin-guide/cgroup-v2.html)
- [Kubernetes: Resource Management for Pods and Containers](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)
- [Kubernetes: Node-pressure Eviction](https://kubernetes.io/docs/concepts/scheduling-eviction/node-pressure-eviction/)
- [Kubernetes: About cgroup v2](https://kubernetes.io/docs/concepts/architecture/cgroups/)
- [Kubernetes: Pod Quality of Service Classes](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/)
- [Kubernetes: Volumes — `emptyDir`](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir)
- [JDK 21: the `java` command](https://docs.oracle.com/en/java/javase/21/docs/specs/man/java.html)
- [JDK 21: `jdk.internal.misc.VM` — the direct-memory default](https://github.com/openjdk/jdk21u/blob/master/src/java.base/share/classes/jdk/internal/misc/VM.java)
- [Netty 4.2: `PlatformDependent` — Netty's direct-memory counter and its choice of allocation path](https://github.com/netty/netty/blob/4.2/common/src/main/java/io/netty/util/internal/PlatformDependent.java), and [`CleanerJava9`](https://github.com/netty/netty/blob/4.2/common/src/main/java/io/netty/util/internal/CleanerJava9.java) — the JDK's way, freed explicitly
- [DuckDB: `config.cpp`](https://github.com/duckdb/duckdb/blob/main/src/main/config.cpp) and [`cgroups.cpp`](https://github.com/duckdb/duckdb/blob/main/src/common/cgroups.cpp) — the default `memory_limit`
- [DuckDB: limits](https://duckdb.org/docs/current/operations_manual/limits.html) and [OOM errors](https://duckdb.org/docs/lts/guides/troubleshooting/oom_errors)
- [Netty 4.2: `PlatformDependent0` — `io.netty.tryReflectionSetAccessible`, the no-`Cleaner` constructor](https://github.com/netty/netty/blob/4.2/common/src/main/java/io/netty/util/internal/PlatformDependent0.java)
- [Netty 4.2: `PoolArena` — direct arenas, chunk allocation and release](https://github.com/netty/netty/blob/4.2/buffer/src/main/java/io/netty/buffer/PoolArena.java), and [`PooledByteBufAllocator`](https://github.com/netty/netty/blob/4.2/buffer/src/main/java/io/netty/buffer/PooledByteBufAllocator.java) — the sizing defaults
- [Netty 4.2: `DirectCleaner`](https://github.com/netty/netty/blob/4.2/common/src/main/java/io/netty/util/internal/DirectCleaner.java) — the no-`Cleaner` way's counter and free; [`CleanerJava24Linker`](https://github.com/netty/netty/blob/4.2/common/src/main/java/io/netty/util/internal/CleanerJava24Linker.java) and [`CleanerJava25`](https://github.com/netty/netty/blob/4.2/common/src/main/java/io/netty/util/internal/CleanerJava25.java) — the FFM fallbacks
- [JDK 25: `sun.nio.ch.Util` — the per-thread temporary direct-buffer cache, `jdk.nio.maxCachedBufferSize`](https://github.com/openjdk/jdk25u/blob/master/src/java.base/share/classes/sun/nio/ch/Util.java)
- [JDK 25: `java.lang.foreign.Arena`](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/foreign/Arena.html) — arena kinds, and zero-initialised allocation
- [JDK 25: `jdk.internal.foreign.ArenaImpl`](https://github.com/openjdk/jdk25u/blob/master/src/java.base/share/classes/jdk/internal/foreign/ArenaImpl.java) — only `Arena.ofAuto()` reserves direct memory
- [JDK 21: `java.nio.Bits` — direct-memory reservation](https://github.com/openjdk/jdk21u/blob/master/src/java.base/share/classes/java/nio/Bits.java) and [`Direct-X-Buffer.java.template` — `DirectByteBuffer`'s `Cleaner`](https://github.com/openjdk/jdk21u/blob/master/src/java.base/share/classes/java/nio/Direct-X-Buffer.java.template)
- Apache Arrow 19, `arrow-memory-netty`: `NettyAllocationManager` and `io.netty.buffer.PooledByteBufAllocatorL`
- [`mmap(2)`](https://man7.org/linux/man-pages/man2/mmap.2.html) — anonymous mappings are zero-initialised; Linux `mm/memory.c` `do_anonymous_page` — a read fault maps the shared zero page, a write fault allocates and zeroes a page of the process's own
- Apache Arrow Java 19: `BaseFixedWidthVector` and `BaseVariableWidthVector` — which buffers `allocateNew` zeroes
- [`mallopt(3)`](https://man7.org/linux/man-pages/man3/mallopt.3.html) — glibc's thresholds, their dynamic adjustment, and the `MALLOC_*` environment variables
- glibc `malloc/arena.c` and `malloc/malloc.c`, tags `glibc-2.43` and `glibc-2.44` (fetched 2026-11-10) — the arena limit, trimming, `malloc_trim`, and the threshold raised on `free` (`DEFAULT_MMAP_THRESHOLD_MAX`; `mallopt(3)` misprints it)
- [JDK 25: `globals.hpp`](https://github.com/openjdk/jdk25u/blob/master/src/hotspot/share/runtime/globals.hpp) — `TrimNativeHeapInterval`; [`os_linux.cpp`](https://github.com/openjdk/jdk25u/blob/master/src/hotspot/os/linux/os_linux.cpp) — `malloc_trim`, and nothing on musl
- musl `src/malloc/mallocng/meta.h`, `malloc.c` and `free.c` (fetched 2026-11-10) — `MMAP_THRESHOLD`, and unmapping on `free`
- Prior art (fetched 2026-11-10): [PostgreSQL 18 developer options](https://www.postgresql.org/docs/18/runtime-config-developer.html) and [resource settings](https://www.postgresql.org/docs/18/runtime-config-resource.html); [MySQL 8.4 `innodb_flush_method`](https://dev.mysql.com/doc/refman/8.4/en/innodb-parameters.html#sysvar_innodb_flush_method) and [`innodb_dedicated_server`](https://dev.mysql.com/doc/refman/8.4/en/innodb-dedicated-server.html); DuckDB `src/common/local_file_system.cpp`, `src/main/config.cpp`, `src/common/cgroups.cpp`; ClickHouse [local filesystem read settings](https://clickhouse.com/docs/reference/settings/session-settings/local-filesystem.md), [`min_bytes_*`](https://clickhouse.com/docs/reference/settings/session-settings/min-bytes.md), [`max_server_memory_usage`](https://clickhouse.com/docs/reference/settings/server-settings/settings/max-server-memory-usage); Seastar `src/core/reactor.cc`, `src/core/resource.cc`; [Cassandra `cassandra.yaml`](https://cassandra.apache.org/doc/latest/cassandra/managing/configuration/cass_yaml_file.html) and [4.1 `Config.java`](https://github.com/apache/cassandra/blob/cassandra-4.1/src/java/org/apache/cassandra/config/Config.java); RocksDB [`options.h`](https://github.com/facebook/rocksdb/blob/main/include/rocksdb/options.h), [Direct IO](https://github.com/facebook/rocksdb/wiki/Direct-IO), [Block Cache](https://github.com/facebook/rocksdb/wiki/Block-Cache); [MongoDB WiredTiger](https://www.mongodb.com/docs/manual/core/wiredtiger/); SQLite [mmap](https://sqlite.org/mmap.html) and [`cache_size`](https://sqlite.org/pragma.html#pragma_cache_size); Elasticsearch [store types](https://www.elastic.co/docs/reference/elasticsearch/index-settings/store) and [search speed](https://www.elastic.co/docs/deploy-manage/production-guidance/optimize-performance/search-speed); [Kafka 4.2 design](https://kafka.apache.org/42/design/design/); QuestDB [storage model](https://questdb.com/docs/concept/storage-model/), [memory management](https://questdb.com/docs/architecture/memory-management/), [memory limits](https://questdb.com/docs/configuration/cairo-engine/#memory-limits); [LMDB `lmdb.h`](https://github.com/LMDB/lmdb/blob/mdb.master/libraries/liblmdb/lmdb.h); Neo4j [configuration settings](https://neo4j.com/docs/operations-manual/current/configuration/configuration-settings/) and [disks, RAM and other tips](https://neo4j.com/docs/operations-manual/current/performance/disks-ram-and-other-tips/)
