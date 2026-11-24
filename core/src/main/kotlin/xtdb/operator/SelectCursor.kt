package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.ICursor
import xtdb.arrow.MaskedRelation
import xtdb.arrow.RelationReader
import xtdb.util.closeAll
import java.util.function.Consumer

class SelectCursor(
    private val al: BufferAllocator,
    private val inCursor: ICursor,
    private val selector: SelectionSpec,
) : ICursor {

    override val cursorType get() = "select"
    override val childCursors get() = listOf(inCursor)
    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
        var advanced = false

        while (!advanced) {
            inCursor.tryAdvance { inRel ->
                val mask = selector.select(inRel)
                when (mask.cardinality) {
                    0 -> {}

                    inRel.rowCount -> {
                        c.accept(inRel)
                        advanced = true
                    }

                    else -> {
                        c.accept(MaskedRelation.of(inRel, mask))
                        advanced = true
                    }
                }
            } || break
        }

        return advanced
    }

    override fun close() = listOf(selector, inCursor).closeAll()
}