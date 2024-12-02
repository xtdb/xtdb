package xtdb.cache

interface Stats {
    val pinnedBytes: Long
    val evictableBytes: Long
    val freeBytes: Long


    companion object {

        operator fun invoke (pinnedBytes: Long, evictableBytes: Long, freeBytes: Long): Stats {
            return object : Stats {
                override val pinnedBytes: Long = pinnedBytes
                override val evictableBytes: Long = evictableBytes
                override val freeBytes: Long = freeBytes
            }
        }

        fun compareStats(a: Stats, b: Stats): Boolean {
            return a.pinnedBytes == b.pinnedBytes && a.evictableBytes == b.evictableBytes && a.freeBytes == b.freeBytes
        }
    }
}
