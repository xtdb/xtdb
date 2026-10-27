package xtdb.cache

import java.util.concurrent.atomic.LongAdder

/**
 * S3-FIFO eviction (Yang et al., "FIFO queues are all you need for cache eviction", SOSP '23), weighted by bytes,
 * over entries that may be held - a held entry is never evicted.
 *
 * An entry read once passes through `small`; one read again while there moves to `main`, which is a CLOCK;
 * `ghost` remembers the keys of recent evictions from `small`, so an entry admitted again soon after goes straight to `main`.
 *
 * With [detectLoops], `ghost` also remembers evictions from `main`, for twice as long.
 * An entry evicted from `main` and then admitted again is the sign of a loop over more than the cache holds, which would
 * cycle through `main` in the order it rereads it, and starts a trial freeze of `main`: nothing is promoted into it and
 * nothing is evicted from it, so it keeps a stable part of the loop while `small` takes the rest.
 * A freeze lasts while each window of reads hits more often than the window before it froze - re-measured every
 * [PROBE_WINDOWS] windows - and ends if nothing but `main` can be evicted; one that ends without paying waits out a
 * doubling number of windows before the next.
 *
 * `ghost` doesn't remember an entry evicted [unread][Entry.isUnread]: admitting it again is its first read.
 *
 * Not thread-safe: its owner serialises every call but [hit].
 */
internal class S3Fifo<K : Any, E : S3Fifo.Entry<K>>(maxBytes: Long, private val detectLoops: Boolean = true) {

    interface Entry<K> {
        val key: K
        val bytes: Long

        /**
         * Raised by [hit], up to [MAX_FREQ].
         */
        var freq: Int

        val isHeld: Boolean

        /**
         * Admitted ahead of its first read, which hasn't come yet.
         */
        val isUnread: Boolean get() = false
    }

    private enum class EvictedFrom { SMALL, MAIN }

    private val small = ArrayDeque<E>()
    private val main = ArrayDeque<E>()
    private var smallBytes = 0L
    private val smallTargetBytes = maxBytes / SMALL_QUEUE_RATIO

    // with loops to detect, keys for twice the resident entries - with as many as there are resident, a loop of more than
    // about twice the cache would be gone from it before it came round again
    private val ghostFactor = if (detectLoops) 2 else 1

    private val ghost = object : LinkedHashMap<K, EvictedFrom>() {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, EvictedFrom>) =
            size > maxOf(MIN_GHOST_CAPACITY, ghostFactor * (small.size + main.size))
    }

    var isFrozen = false
        private set

    // the hit rate of the last window that wasn't frozen - what a freeze has to beat
    private var baselineHitRate = 0.0
    private var failedFreezes = 0
    private var backoffWindows = 0
    private var frozenWindows = 0

    private val hits = LongAdder()
    private var windowStartHits = 0L
    private var windowMisses = 0L

    val entries: Sequence<E> get() = small.asSequence() + main.asSequence()

    /**
     * Called by the owner on a hit, on the reader's thread and without the owner's lock:
     * a lost `freq` increment only blurs the eviction priority.
     */
    fun hit(entry: E) {
        if (entry.freq < MAX_FREQ) entry.freq++
        hits.increment()
    }

    fun admit(entry: E) {
        val evictedFrom = ghost.remove(entry.key)

        if (!isFrozen && evictedFrom == EvictedFrom.MAIN && backoffWindows == 0) freeze()

        windowMisses++

        if (evictedFrom == EvictedFrom.SMALL && !isFrozen) main.addLast(entry)
        else {
            small.addLast(entry); smallBytes += entry.bytes
        }

        if (windowMisses % WINDOW_CHECK_MISSES == 0L) endWindowIfDue()
    }

    private fun startWindow() {
        windowStartHits = hits.sum()
        windowMisses = 0
    }

    private fun freeze() {
        val windowHits = hits.sum() - windowStartHits
        val reads = windowHits + windowMisses
        if (reads >= MIN_WINDOW_READS) baselineHitRate = windowHits.toDouble() / reads

        isFrozen = true
        frozenWindows = 0
        startWindow()
    }

    private fun thaw(paid: Boolean) {
        isFrozen = false
        // one that paid still waits a window, so the next freeze is judged against a hit rate measured since this one
        backoffWindows = if (paid) 1 else 1 shl minOf(++failedFreezes, MAX_BACKOFF_SHIFT)
        startWindow()
    }

    private fun endWindowIfDue() {
        val windowHits = hits.sum() - windowStartHits
        val reads = windowHits + windowMisses
        if (reads < maxOf(MIN_WINDOW_READS, 2L * (small.size + main.size))) return

        val hitRate = windowHits.toDouble() / reads

        when {
            !isFrozen -> {
                baselineHitRate = hitRate
                if (backoffWindows > 0) backoffWindows--
                startWindow()
            }

            hitRate <= baselineHitRate -> thaw(paid = false)

            else -> {
                failedFreezes = 0
                if (++frozenWindows >= PROBE_WINDOWS) thaw(paid = true) else startWindow()
            }
        }
    }

    /**
     * @return the entry it removed, for the owner to free - or null if every entry is held
     */
    fun evict(): E? {
        // held entries each queue has rotated past since it last made progress - once one has been all the way round, use the other
        var smallSkips = 0
        var mainSkips = 0

        // hits raise `freq` without the owner's lock, so this bounds the scan even if they keep pace with it
        var visits = (MAX_FREQ + 2) * (small.size + main.size) + 1

        while (visits-- > 0) {
            val smallLive = small.isNotEmpty() && smallSkips < small.size
            val mainLive = main.isNotEmpty() && mainSkips < main.size

            if (smallLive && (smallBytes >= smallTargetBytes || !mainLive || isFrozen)) {
                val entry = small.removeFirst().also { smallBytes -= it.bytes }
                when {
                    entry.freq > 0 && !isFrozen -> {
                        smallSkips = 0
                        entry.freq = 0
                        main.addLast(entry)
                    }

                    !entry.isHeld -> {
                        if (!entry.isUnread) ghost[entry.key] = EvictedFrom.SMALL
                        return entry
                    }

                    else -> {
                        smallSkips++
                        small.addLast(entry); smallBytes += entry.bytes
                    }
                }
            } else if (mainLive) {
                val entry = main.removeFirst()
                when {
                    // a held entry keeps its frequency: it can't be evicted, so ageing it would only count against it later
                    entry.isHeld -> mainSkips++

                    entry.freq > 0 -> {
                        mainSkips = 0
                        entry.freq--
                    }

                    else -> {
                        if (detectLoops && !entry.isUnread) ghost[entry.key] = EvictedFrom.MAIN
                        if (isFrozen) thaw(paid = false)
                        return entry
                    }
                }
                main.addLast(entry)
            } else return null
        }

        return null
    }

    fun clear() {
        small.clear(); main.clear(); smallBytes = 0
        ghost.clear()
        isFrozen = false
    }

    companion object {
        const val MAX_FREQ = 3
        private const val SMALL_QUEUE_RATIO = 10
        private const val MIN_GHOST_CAPACITY = 16
        private const val MIN_WINDOW_READS = 64L
        private const val WINDOW_CHECK_MISSES = 8L
        private const val PROBE_WINDOWS = 16
        private const val MAX_BACKOFF_SHIFT = 6
    }
}
