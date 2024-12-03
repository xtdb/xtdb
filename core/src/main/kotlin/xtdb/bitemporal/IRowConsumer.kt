package xtdb.bitemporal

@Suppress("unused")
interface IRowConsumer {
    fun accept(validFrom: Long, validTo: Long, systemFrom: Long, systemTo: Long)
}
