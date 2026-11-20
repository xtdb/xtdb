package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.ICursor
import xtdb.arrow.MaskedVector
import xtdb.arrow.RelationReader
import java.util.function.Consumer

class SelectCursor(
    private val al: BufferAllocator,
    private val inCursor: ICursor,
    private val selector: RowMaskSpec,
    private val schema: Map<String, Any>,
    private val args: RelationReader
) : ICursor {

    override val cursorType get() = "select"
    override val childCursors get() = listOf(inCursor)
    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
        var advanced = false

        while (!advanced) {
            inCursor.tryAdvance { inRel ->
                val mask = selector.rowMask(al, inRel, schema, args)
                when (mask.cardinality) {
                    0 -> {}

                    inRel.rowCount -> {
                        c.accept(inRel)
                        advanced = true
                    }

                    else -> {
                        c.accept(RelationReader.from(inRel.vectors.map { MaskedVector(it, mask) }, mask.cardinality))
                        advanced = true
                    }
                }
            } || break
        }

        return advanced
    }

    override fun close() = inCursor.close()
}