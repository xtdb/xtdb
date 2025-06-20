package xtdb.operator.apply

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.vector.NullVector
import org.apache.arrow.vector.types.pojo.Field
import xtdb.ICursor
import xtdb.error.Incorrect
import xtdb.trie.ColumnName
import xtdb.types.Fields.BOOL
import xtdb.vector.IRelationWriter
import xtdb.arrow.RelationReader
import xtdb.vector.ValueVectorReader

sealed interface ApplyMode {
    fun accept(
        dependentCursor: ICursor<RelationReader>, dependentOutWriter: IRelationWriter, idxs: IntArrayList, inIdx: Int
    )

    class MarkJoin(private val columnName: ColumnName) : ApplyMode {
        override fun accept(
            dependentCursor: ICursor<RelationReader>, dependentOutWriter: IRelationWriter,
            idxs: IntArrayList, inIdx: Int
        ) {
            idxs.add(inIdx)
            val outWriter = dependentOutWriter.vectorFor(columnName, BOOL.nullable.fieldType)
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

    class CrossJoin(private val dependentFields: List<Field>) : ApplyMode {
        override fun accept(
            dependentCursor: ICursor<RelationReader>, dependentOutWriter: IRelationWriter,
            idxs: IntArrayList, inIdx: Int
        ) {
            dependentFields.forEach { dependentOutWriter.vectorFor(it.name, it.fieldType) }

            dependentCursor.forEachRemaining { depRel ->
                dependentOutWriter.append(depRel)
                repeat(depRel.rowCount) { idxs.add(inIdx) }
            }
        }
    }

    class LeftJoin(private val dependentFields: List<Field>) : ApplyMode {
        override fun accept(
            dependentCursor: ICursor<RelationReader>, dependentOutWriter: IRelationWriter,
            idxs: IntArrayList, inIdx: Int
        ) {
            dependentFields.forEach { dependentOutWriter.vectorFor(it.name, it.fieldType) }

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
                for (field in dependentFields) {
                    dependentOutWriter.vectorFor(field.name, field.fieldType)
                        .append(ValueVectorReader.from(NullVector(field.name).also { it.valueCount = 1 }))
                }
            }
        }
    }

    data object SemiJoin : ApplyMode {
        override fun accept(
            dependentCursor: ICursor<RelationReader>, dependentOutWriter: IRelationWriter,
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
            dependentCursor: ICursor<RelationReader>, dependentOutWriter: IRelationWriter,
            idxs: IntArrayList, inIdx: Int
        ) {
            var match = false

            while (!match) {
                dependentCursor.tryAdvance { depRel -> if (depRel.rowCount > 0) match = true } || break
            }

            if (!match) idxs.add(inIdx)
        }
    }

    class SingleJoin(private val dependentFields: List<Field>) : ApplyMode {

        private val cardinalityViolation: Nothing
            get() =
                throw Incorrect(message = "cardinality violation", errorCode = "xtdb.single-join/cardinality-violation")

        override fun accept(
            dependentCursor: ICursor<RelationReader>, dependentOutWriter: IRelationWriter,
            idxs: IntArrayList, inIdx: Int
        ) {
            dependentFields.forEach { dependentOutWriter.vectorFor(it.name, it.fieldType) }

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
                for (field in dependentFields) {
                    dependentOutWriter.vectorFor(field.name, field.fieldType)
                        .append(ValueVectorReader.from(NullVector(field.name).also { it.valueCount = 1 }))
                }
            }
        }
    }
}