package xtdb.expression.map

interface RelationMapBuilder {
    fun add(inIdx: Int)
    fun addIfNotPresent(inIdx: Int): Int
}