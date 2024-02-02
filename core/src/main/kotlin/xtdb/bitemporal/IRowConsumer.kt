package xtdb.bitemporal

@Suppress("unused")
interface IRowConsumer {
    fun accept(idx: Int, validFrom: Long, validTo: Long, systemFrom: Long, systemTo: Long)
}
