package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.ICursor
import xtdb.arrow.RelationReader
import java.util.function.Consumer

class SelectCursor(
    private val al: BufferAllocator,
    private val inCursor: ICursor<RelationReader>,
    private val selector: SelectionSpec,
    private val schema: Map<String, Any>,
    private val args: RelationReader
) : ICursor<RelationReader> {
    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
        var advanced = false

        while (!advanced) {
            inCursor.tryAdvance { inRel ->
                val sel = selector.select(al, inRel, schema, args)
                if (sel.isNotEmpty()) {
                    c.accept(inRel.select(sel))
                    advanced = true
                }
            } || break
        }

        return advanced
    }

    override fun close() = inCursor.close()
}