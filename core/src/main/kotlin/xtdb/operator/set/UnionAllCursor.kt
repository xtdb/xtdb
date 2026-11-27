package xtdb.operator.set

import xtdb.api.ICursor
import xtdb.arrow.RelationReader
import xtdb.util.closeAll
import java.util.function.Consumer

class UnionAllCursor(private val left: ICursor, private val right: ICursor) : ICursor {
    override val cursorType get() = "union-all"
    override val childCursors get() = listOf(left, right)

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
        var advanced = false
        val acceptNonEmpty = Consumer<RelationReader> { rel ->
            if (rel.rowCount > 0) {
                advanced = true
                c.accept(rel)
            }
        }

        while (!advanced) {
            if (!left.tryAdvance(acceptNonEmpty) && !right.tryAdvance(acceptNonEmpty)) return false
        }

        return true
    }

    override fun close() = listOf(left, right).closeAll()
}
