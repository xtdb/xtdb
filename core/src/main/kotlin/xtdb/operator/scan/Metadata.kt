package xtdb.operator.scan

import xtdb.log.proto.TemporalMetadata

interface Metadata {
    fun testMetadata(): Boolean
    val temporalMetadata: TemporalMetadata
}