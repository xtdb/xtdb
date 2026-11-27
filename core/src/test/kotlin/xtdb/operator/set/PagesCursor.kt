package xtdb.operator.set

import org.apache.arrow.memory.BufferAllocator
import xtdb.api.ICursor
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorTypes
import java.util.function.Consumer

internal class PagesCursor(
    private val al: BufferAllocator,
    private val vecTypes: VectorTypes,
    pages: List<List<Map<String, *>>>,
) : ICursor {
    private val pages = pages.iterator()

    override val cursorType get() = "pages"
    override val childCursors get() = emptyList<ICursor>()

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
        if (!pages.hasNext()) return false
        Relation(al, vecTypes).use { rel ->
            for (row in pages.next()) rel.writeRow(row)
            c.accept(rel)
        }
        return true
    }
}
