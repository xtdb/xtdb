package xtdb.operator.apply

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.memory.BufferAllocator
import xtdb.ICursor
import xtdb.arrow.RelationReader
import xtdb.vector.RelationWriter
import java.util.function.Consumer

class ApplyCursor(
    private val al: BufferAllocator,
    private val mode: ApplyMode,
    private val independentCursor: ICursor<RelationReader>,
    private val depCursorFactory: DependentCursorFactory
) : ICursor<RelationReader> {
    override fun tryAdvance(c: Consumer<in RelationReader>) =
        independentCursor.tryAdvance { inRel ->
            val idxs = IntArrayList()
            RelationWriter(al).use { depOutWriter ->
                repeat(inRel.rowCount) { inIdx ->
                    depCursorFactory.open(inRel, inIdx).use { depCursor ->
                        mode.accept(depCursor, depOutWriter, idxs, inIdx)
                    }
                }

                val sel = idxs.toArray()
                val cols = inRel.vectors.map { it.select(sel) } + depOutWriter.asReader.vectors
                c.accept(RelationReader.from(cols, sel.size))
            }
        }

    override fun close() = independentCursor.close()
}