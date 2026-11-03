package xtdb.util

import java.time.Duration
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.StampedLock
import kotlin.concurrent.Volatile

class RefCounter {
    private val lock = StampedLock()
    private val refs = AtomicInteger(0)

    @Volatile
    private var closingSemaphore: Semaphore? = null

    fun acquire() {
        lock.withReadLock {
            check(closingSemaphore == null) { "node closing" }
            refs.incrementAndGet()
        }
    }

    fun release() {
        lock.withReadLock {
            refs.decrementAndGet()
            closingSemaphore?.release()
        }
    }

    val isClosing: Boolean
        get() = closingSemaphore != null

    @Throws(Exception::class)
    fun tryClose(timeout: Duration?): Boolean {
        val openQueries: Int

        val closingSemaphore = lock.withWriteLock {
            check(closingSemaphore == null) { "node closing" }
            openQueries = refs.get()
            Semaphore(0).also { this.closingSemaphore = it }
        }

        return if (timeout != null)
            closingSemaphore.tryAcquire(openQueries, timeout.toSeconds(), TimeUnit.SECONDS)
        else
            closingSemaphore.tryAcquire(openQueries)
    }
}
