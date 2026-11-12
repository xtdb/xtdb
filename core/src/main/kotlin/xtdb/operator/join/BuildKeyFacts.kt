package xtdb.operator.join

import xtdb.arrow.FieldName
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorType

/**
 * What an uncorrelated `IN` / `NOT IN` needs to know about its whole build side to mark a probe row no build row matched,
 * gathered from each relation as it is appended - before a spill partitions it, since every NULL key hashes alike.
 */
class BuildKeyFacts(private val buildKeyColName: FieldName) {
    var isEmpty = true; private set
    var hasNullKey = false; private set

    /**
     * True once a key value could be a list or a struct, whose `=` is unknown on an inner NULL without the key being NULL:
     * `[1, NULL] = [1, 2]` is NULL. The facts can't see that, so the join compares every row instead.
     */
    var hasNestedKey = false; private set

    fun add(buildRel: RelationReader) {
        if (buildRel.rowCount == 0) return
        isEmpty = false

        val keyCol = buildRel[buildKeyColName]
        if (!hasNestedKey) hasNestedKey = keyCol.type.legs.any { it is VectorType.Listy || it is VectorType.Struct }
        if (!hasNullKey) hasNullKey = (0 until buildRel.rowCount).any { keyCol.isNull(it) }
    }

    /** The mark of a probe row whose key no build row matched: FALSE against an empty build side, NULL where either side holds a NULL key. */
    fun unmatchedMark(probeKeyIsNull: Boolean): Int = when {
        isEmpty -> -1
        probeKeyIsNull || hasNullKey -> 0
        else -> -1
    }
}
