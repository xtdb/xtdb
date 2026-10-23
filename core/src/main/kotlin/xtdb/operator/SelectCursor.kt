package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.ICursor
import xtdb.arrow.RelationReader
import java.util.function.Consumer

class SelectCursor(
    private val al: BufferAllocator,
    private val inCursor: ICursor,
    private val selector: SelectionSpec,
    private val schema: Map<String, Any>,
    private val args: RelationReader
) : ICursor {

    override val cursorType get() = "select"
    override val childCursors get() = listOf(inCursor)
    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
        var advanced = false

        while (!advanced) {
            inCursor.tryAdvance { inRel ->
                val outRel = SelectionSpec.selectBy(inRel, selector, al, schema, args)
                if (outRel.rowCount > 0) {
                    c.accept(outRel)
                    advanced = true
                }
            } || break
        }

        return advanced
    }

    override fun close() = inCursor.close()
}