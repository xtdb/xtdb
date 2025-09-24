package xtdb.operator.join

import xtdb.arrow.RelationReader
import xtdb.arrow.VectorReader
import xtdb.types.FieldName
import java.util.function.IntBinaryOperator

interface ComparatorFactory {
    companion object {
        private fun andIBO(p1: IntBinaryOperator, p2: IntBinaryOperator) = IntBinaryOperator { l, r ->
            val lRes = p1.applyAsInt(l, r)
            if (lRes == -1) -1 else minOf(lRes, p2.applyAsInt(l, r))
        }

        @JvmStatic
        fun ComparatorFactory.build(
            buildSide: BuildSide, probeRel: RelationReader, probeKeyColNames: List<FieldName>
        ): IntBinaryOperator {
            val buildRel = buildSide.dataRel

            val buildKeyCols = buildSide.keyColNames.map { buildRel[it] }
            val probeKeyCols = probeKeyColNames.map { probeRel[it] }

            return buildKeyCols.zip(probeKeyCols)
                .map { (buildCol, probeCol) -> buildEqui(buildCol, probeCol) }
                .plus(listOfNotNull(buildTheta(buildRel, probeRel)))
                .reduceOrNull(::andIBO)
                ?: IntBinaryOperator { _, _ -> 1 }
        }
    }

    fun buildEqui(buildCol: VectorReader, probeCol: VectorReader): IntBinaryOperator
    fun buildTheta(buildRel: RelationReader, probeRel: RelationReader): IntBinaryOperator?
}