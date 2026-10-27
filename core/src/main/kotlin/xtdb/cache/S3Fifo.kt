package xtdb.cache

/**
 * S3-FIFO eviction (Yang et al., "FIFO queues are all you need for cache eviction", SOSP '23), weighted by bytes,
 * over entries that may be held - a held entry is never evicted.
 *
 * An entry read once passes through `small`; one read again while there moves to `main`, which is a CLOCK;
 * `ghost` remembers the keys of recent evictions from `small`, so an entry admitted again soon after goes straight to `main`.
 *
 * Not thread-safe: its owner serialises every call.
 * [Entry.freq] is the exception - hits raise it without the owner's lock.
 */
internal class S3Fifo<K : Any, E : S3Fifo.Entry<K>>(maxBytes: Long) {

    interface Entry<K> {
        val key: K
        val bytes: Long

        /**
         * Raised by the owner on each hit, up to [MAX_FREQ]; a lost increment only blurs the eviction priority.
         */
        var freq: Int

        val isHeld: Boolean
    }

    private val small = ArrayDeque<E>()
    private val main = ArrayDeque<E>()
    private var smallBytes = 0L
    private val smallTargetBytes = maxBytes / SMALL_QUEUE_RATIO

    private val ghost = object : LinkedHashMap<K, Unit>() {
        override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, Unit>) =
            size > maxOf(MIN_GHOST_CAPACITY, small.size + main.size)
    }

    val entries: Sequence<E> get() = small.asSequence() + main.asSequence()

    fun admit(entry: E) {
        if (ghost.remove(entry.key) != null) main.addLast(entry)
        else {
            small.addLast(entry); smallBytes += entry.bytes
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

            if (smallLive && (smallBytes >= smallTargetBytes || !mainLive)) {
                val entry = small.removeFirst().also { smallBytes -= it.bytes }
                when {
                    entry.freq > 0 -> {
                        smallSkips = 0
                        entry.freq = 0
                        main.addLast(entry)
                    }

                    !entry.isHeld -> {
                        ghost[entry.key] = Unit
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

                    else -> return entry
                }
                main.addLast(entry)
            } else return null
        }

        return null
    }

    fun clear() {
        small.clear(); main.clear(); smallBytes = 0
        ghost.clear()
    }

    companion object {
        const val MAX_FREQ = 3
        private const val SMALL_QUEUE_RATIO = 10
        private const val MIN_GHOST_CAPACITY = 16
    }
}
