package xtdb.operator.apply

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.memory.BufferAllocator
import xtdb.ICursor
import xtdb.arrow.FieldName
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorType
import java.util.function.Consumer

class ApplyCursor(
    private val al: BufferAllocator,
    private val mode: ApplyMode,
    private val independentCursor: ICursor,
    private val depVecTypes: Map<FieldName, VectorType>,
    private val depCursorFactory: DependentCursorFactory
) : ICursor {

    override val cursorType get() = when (mode) {
        is ApplyMode.MarkJoin -> "apply-mark-join"
        is ApplyMode.CrossJoin -> "apply-cross-join"
        is ApplyMode.LeftJoin -> "apply-left-join"
        is ApplyMode.SemiJoin -> "apply-semi-join"
        is ApplyMode.AntiJoin -> "apply-anti-join"
        is ApplyMode.SingleJoin -> "apply-single-join"
    }
    override val childCursors get() = listOf(independentCursor)
    override fun tryAdvance(c: Consumer<in RelationReader>) =
        independentCursor.tryAdvance { inRel ->
            val idxs = IntArrayList()
            Relation(al, depVecTypes.map { (name, type) -> type.toField(name) }).use { depOutWriter ->
                repeat(inRel.rowCount) { inIdx ->
                    depCursorFactory.open(inRel, inIdx).use { depCursor ->
                        mode.accept(depCursor, depOutWriter, idxs, inIdx)
                    }
                }

                val sel = idxs.toArray()
                val cols = inRel.vectors.map { it.select(sel) } + depOutWriter.vectors
                c.accept(RelationReader.from(cols, sel.size))
            }
        }

    override fun close() = independentCursor.close()
}