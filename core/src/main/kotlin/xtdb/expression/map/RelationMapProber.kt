package xtdb.expression.map

import java.util.function.IntConsumer

interface RelationMapProber {
    fun indexOf(inIdx: Int, removeOnMatch: Boolean): Int
    fun forEachMatch(inIdx: Int, c: IntConsumer)
    fun matches(inIdx: Int): Int
}