package xtdb.operator.join

import xtdb.arrow.FieldName
import xtdb.arrow.RelationReader
import java.time.ZoneId
import java.util.function.IntBinaryOperator

class ComparatorFactory(private val nullsEqual: Boolean, private val zone: ZoneId, private val theta: Theta?) {

    fun interface Theta {
        fun build(buildRel: RelationReader, probeRel: RelationReader): IntBinaryOperator
    }

    fun build(buildSide: BuildSide, probeRel: RelationReader, probeKeyColNames: List<FieldName>): IntBinaryOperator {
        val buildRel = buildSide.dataRel

        val buildKeyCols = buildSide.keyColNames.map { buildRel[it] }
        val probeKeyCols = probeKeyColNames.map { probeRel[it] }

        return buildKeyCols.zip(probeKeyCols)
            .map { (buildCol, probeCol) -> buildCol.equiComparator(probeCol, nullsEqual, zone) }
            .plus(listOfNotNull(theta?.build(buildRel, probeRel)))
            .reduceOrNull(::andIBO)
            ?: IntBinaryOperator { _, _ -> 1 }
    }

    private companion object {
        fun andIBO(p1: IntBinaryOperator, p2: IntBinaryOperator) = IntBinaryOperator { l, r ->
            val lRes = p1.applyAsInt(l, r)
            if (lRes == -1) -1 else minOf(lRes, p2.applyAsInt(l, r))
        }
    }
}
