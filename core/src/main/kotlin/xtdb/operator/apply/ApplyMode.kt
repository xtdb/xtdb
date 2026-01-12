package xtdb.operator.apply

import com.carrotsearch.hppc.IntArrayList
import xtdb.ICursor
import xtdb.arrow.FieldName
import xtdb.arrow.NullVector
import xtdb.arrow.RelationWriter
import xtdb.arrow.VectorType
import xtdb.error.Incorrect
import xtdb.trie.ColumnName
import xtdb.arrow.VectorType.Companion.BOOL

sealed interface ApplyMode {
    fun accept(
        dependentCursor: ICursor, dependentOutWriter: RelationWriter, idxs: IntArrayList, inIdx: Int
    )

    class MarkJoin(private val columnName: ColumnName) : ApplyMode {
        override fun accept(
            dependentCursor: ICursor, dependentOutWriter: RelationWriter,
            idxs: IntArrayList, inIdx: Int
        ) {
            idxs.add(inIdx)
            val outWriter = dependentOutWriter.vectorFor(columnName, BOOL.arrowType, true)
            var match = -1
            while (match != 1) {
                dependentCursor.tryAdvance { depRel ->
                    val matchVec = depRel.vectorFor("_expr")
                    repeat(matchVec.valueCount) { idx ->
                        match = match.coerceAtLeast(
                            if (matchVec.isNull(idx)) 0 else if (matchVec.getBoolean(idx)) 1 else -1
                        )
                    }
                } || break
            }

            if (match == 0) outWriter.writeNull() else outWriter.writeBoolean(match == 1)
        }
    }

    class CrossJoin(private val dependentVecTypes: Map<FieldName, VectorType>) : ApplyMode {
        override fun accept(
            dependentCursor: ICursor, dependentOutWriter: RelationWriter,
            idxs: IntArrayList, inIdx: Int
        ) {
            dependentVecTypes.forEach { (name, type) -> dependentOutWriter.vectorFor(name, type.arrowType, type.nullable) }

            dependentCursor.forEachRemaining { depRel ->
                dependentOutWriter.append(depRel)
                repeat(depRel.rowCount) { idxs.add(inIdx) }
            }
        }
    }

    class LeftJoin(private val dependentVecTypes: Map<FieldName, VectorType>) : ApplyMode {
        override fun accept(
            dependentCursor: ICursor, dependentOutWriter: RelationWriter,
            idxs: IntArrayList, inIdx: Int
        ) {
            dependentVecTypes.forEach { (name, type) -> dependentOutWriter.vectorFor(name, type.arrowType, type.nullable) }

            var match = false
            dependentCursor.forEachRemaining { depRel ->
                if (depRel.rowCount > 0) {
                    match = true
                    dependentOutWriter.append(depRel)
                    repeat(depRel.rowCount) { idxs.add(inIdx) }
                }
            }

            if (!match) {
                idxs.add(inIdx)
                for ((name, _) in dependentVecTypes) {
                    dependentOutWriter.vectorFor(name, VectorType.NULL.arrowType, true)
                        .append(NullVector(name).also { it.valueCount = 1 })
                }
            }
        }
    }

    data object SemiJoin : ApplyMode {
        override fun accept(
            dependentCursor: ICursor, dependentOutWriter: RelationWriter,
            idxs: IntArrayList, inIdx: Int
        ) {
            var match = false

            while (!match) {
                dependentCursor.tryAdvance { depRel ->
                    if (depRel.rowCount > 0) {
                        match = true
                        idxs.add(inIdx)
                    }
                } || break
            }
        }
    }

    data object AntiJoin : ApplyMode {
        override fun accept(
            dependentCursor: ICursor, dependentOutWriter: RelationWriter,
            idxs: IntArrayList, inIdx: Int
        ) {
            var match = false

            while (!match) {
                dependentCursor.tryAdvance { depRel -> if (depRel.rowCount > 0) match = true } || break
            }

            if (!match) idxs.add(inIdx)
        }
    }

    class SingleJoin(private val dependentVecTypes: Map<FieldName, VectorType>) : ApplyMode {

        private val cardinalityViolation: Nothing
            get() =
                throw Incorrect(message = "cardinality violation", errorCode = "xtdb.single-join/cardinality-violation")

        override fun accept(
            dependentCursor: ICursor, dependentOutWriter: RelationWriter,
            idxs: IntArrayList, inIdx: Int
        ) {
            dependentVecTypes.forEach { (name, type) -> dependentOutWriter.vectorFor(name, type.arrowType, type.nullable) }

            var match = false

            dependentCursor.forEachRemaining { depRel ->
                val depRowCount = depRel.rowCount
                when {
                    depRowCount == 0 -> Unit
                    (if (match) 1 else 0) + depRowCount > 1 -> cardinalityViolation
                    else -> {
                        match = true
                        idxs.add(inIdx)
                        dependentOutWriter.append(depRel)
                    }
                }
            }

            if (!match) {
                idxs.add(inIdx)
                for ((name, _) in dependentVecTypes) {
                    dependentOutWriter.vectorFor(name, VectorType.NULL.arrowType, true)
                        .append(NullVector(name).also { it.valueCount = 1 })
                }
            }
        }
    }
}