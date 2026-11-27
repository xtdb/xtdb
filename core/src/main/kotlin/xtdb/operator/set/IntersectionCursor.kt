package xtdb.operator.set

import xtdb.api.ICursor
import xtdb.arrow.RelationReader
import xtdb.operator.join.BuildSide
import xtdb.operator.join.ProbeSide
import xtdb.util.closeAll
import java.util.function.Consumer

class IntersectionCursor(
    private val left: ICursor, private val right: ICursor,
    private val buildSide: BuildSide, private val probeSide: ProbeSide,
    private val mode: Mode,
) : ICursor {

    enum class Mode(val cursorType: String) {
        INTERSECT("intersection") {
            override fun keeps(matched: Boolean) = matched
        },
        DIFFERENCE("difference") {
            override fun keeps(matched: Boolean) = !matched
        };

        abstract fun keeps(matched: Boolean): Boolean
    }

    private var built = false

    override val cursorType get() = mode.cursorType
    override val childCursors get() = listOf(left, right)

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
        if (!built) {
            right.forEachRemaining { buildSide.append(it) }
            buildSide.end()
            built = true
        }

        var advanced = false

        while (!advanced) {
            left.tryAdvance { probeRel ->
                if (probeRel.rowCount == 0) return@tryAdvance

                val batch = probeSide.forBatch(probeRel)
                val idxs = (0 until probeRel.rowCount)
                    .filter { probeIdx -> mode.keeps(batch.indexOf(probeIdx, true) >= 0) }
                    .toIntArray()

                if (idxs.isNotEmpty()) {
                    advanced = true
                    c.accept(probeRel.select(idxs))
                }
            } || break
        }

        return advanced
    }

    override fun close() = listOf(probeSide, buildSide, left, right).closeAll()
}
