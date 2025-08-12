package xtdb.expression.map

interface RelationMapBuilder {
    fun addIfNotPresent(inIdx: Int): Int
}