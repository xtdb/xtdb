package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.ICursor
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
    override fun tryAdvance(c: Consumer<in List<RelationReader>>): Boolean {
        var advanced = false

        while (!advanced) {
            inCursor.tryAdvance { inRels ->
                val outRels = inRels.mapNotNull { inRel ->
                    val sel = selector.select(al, inRel, schema, args)
                    if (sel.isNotEmpty()) inRel.select(sel) else null
                }
                if (outRels.isNotEmpty()) {
                    c.accept(outRels)
                    advanced = true
                }
            } || break
        }

        return advanced
    }

    override fun close() = inCursor.close()
}