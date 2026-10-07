package xtdb.flight_sql

import java.time.Duration
import java.time.Instant
import java.time.InstantSource
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

internal class Lease(
    private val clock: InstantSource,
    private val idleTimeout: Duration,
    firstLimit: Duration = idleTimeout,
) {
    sealed interface State {
        data class Live(val since: Instant, val limit: Duration, val inUse: Int) : State {
            fun lapsedAt(now: Instant): Boolean = inUse == 0 && !now.isBefore(since.plus(limit))
        }

        sealed interface Released : State {
            data object Ended : Released
            data class Expired(val at: Instant, val after: Duration) : Released
        }
    }

    // renew, end and expire each compare-and-set against the state they observed, so of a racing renewal and expiry
    // exactly one wins
    private val state = AtomicReference<State>(State.Live(clock.instant(), firstLimit, 0))

    val current: State get() = state.get()

    private inline fun updateLive(f: (State.Live, Instant) -> State.Live): Boolean {
        while (true) {
            val now = clock.instant()
            val s = state.get() as? State.Live ?: return false
            if (s.lapsedAt(now)) return false
            if (state.compareAndSet(s, f(s, now))) return true
        }
    }

    fun renew(): Boolean = updateLive { s, now -> s.copy(since = now, limit = idleTimeout) }

    fun acquire(): Boolean = updateLive { s, _ -> s.copy(inUse = s.inUse + 1) }

    fun renewAndAcquire(): Boolean =
        updateLive { s, now -> s.copy(since = now, limit = idleTimeout, inUse = s.inUse + 1) }

    fun release(): Boolean = updateLive { s, now -> s.copy(since = now, inUse = s.inUse - 1) }

    fun end(): Boolean {
        while (true) {
            val s = state.get() as? State.Live ?: return false
            if (s.lapsedAt(clock.instant())) return false
            if (state.compareAndSet(s, State.Released.Ended)) return true
        }
    }

    /** @return true if this call expired the lease - the caller then owns releasing what it covered. */
    fun expireIfLapsed(): Boolean {
        val now = clock.instant()
        val s = state.get() as? State.Live ?: return false
        return s.lapsedAt(now) && state.compareAndSet(s, State.Released.Expired(now, s.limit))
    }
}

internal interface Leased : AutoCloseable {
    val lease: Lease
}

internal class Leases<K : Any, V : Leased>(private val clock: InstantSource) : AutoCloseable {

    sealed interface Lookup<out V> {
        data class Found<V>(val value: V) : Lookup<V>
        data class Expired(val after: Duration) : Lookup<Nothing>
        data object Unknown : Lookup<Nothing>
    }

    private val entries = ConcurrentHashMap<K, V>()

    fun add(key: K, value: V): V = value.also { entries[key] = it }

    fun lookup(key: K): Lookup<V> = resolve(key) { it.lease.renew() }

    fun acquire(key: K): Lookup<V> = resolve(key) { it.lease.renewAndAcquire() }

    fun take(key: K): Lookup<V> = resolve(key) { it.lease.end() }.also { if (it is Lookup.Found) entries.remove(key) }

    private inline fun resolve(key: K, claim: (V) -> Boolean): Lookup<V> {
        val v = entries[key] ?: return Lookup.Unknown
        if (claim(v)) return Lookup.Found(v)
        if (v.lease.expireIfLapsed()) v.close()
        return when (val s = v.lease.current) {
            is Lease.State.Released.Expired -> Lookup.Expired(s.after)
            Lease.State.Released.Ended, is Lease.State.Live -> Lookup.Unknown
        }
    }

    fun sweep() {
        val now = clock.instant()
        for ((key, v) in entries) {
            if (v.lease.expireIfLapsed()) v.close()
            else (v.lease.current as? Lease.State.Released.Expired)
                ?.takeIf { !now.isBefore(it.at.plus(it.after)) }
                ?.let { entries.remove(key, v) }
        }
    }

    override fun close() {
        for (v in entries.values) if (v.lease.end() || v.lease.expireIfLapsed()) v.close()
        entries.clear()
    }
}
