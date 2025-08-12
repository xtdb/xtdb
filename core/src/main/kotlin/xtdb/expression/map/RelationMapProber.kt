package xtdb.expression.map

import java.util.function.IntConsumer

interface RelationMapProber {
    fun indexOf(probeIdx: Int, removeOnMatch: Boolean): Int
    fun forEachMatch(probeIdx: Int, c: IntConsumer)
    fun matches(probeIdx: Int): Int
}