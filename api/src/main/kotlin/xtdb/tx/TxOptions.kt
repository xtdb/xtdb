package xtdb.tx

import clojure.lang.*
import java.time.Instant
import java.time.ZoneId

private val SYSTEM_TIME_KEY: Keyword = Keyword.intern("system-time")
private val DEFAULT_TZ_KEY: Keyword = Keyword.intern("default-tz")

data class TxOptions(val systemTime: Instant? = null, private val defaultTz: ZoneId? = null) : ILookup, Seqable {
    fun systemTime(): Instant? = systemTime
    fun defaultTz(): ZoneId? = defaultTz

    override fun valAt(key: Any?): Any? = valAt(key, null)

    override fun valAt(key: Any?, notFound: Any?): Any? =
        when {
            key === SYSTEM_TIME_KEY -> systemTime
            key === DEFAULT_TZ_KEY -> defaultTz
            else -> notFound
        }

    override fun seq(): ISeq =
        PersistentList.create(
            listOfNotNull(
                systemTime?.let { MapEntry.create(SYSTEM_TIME_KEY, it) },
                defaultTz?.let { MapEntry.create(DEFAULT_TZ_KEY, it) })
        ).seq()
}
