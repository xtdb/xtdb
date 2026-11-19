package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.api.ICursor
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorTypes
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import xtdb.util.closeOnCatch
import java.util.function.Consumer

class LetCursorFactory(
    private val al: BufferAllocator, private val boundCursor: ICursor,
    private val boundVecTypes: VectorTypes, private val spillThresholdRows: Long,
) : ICursor.Factory, AutoCloseable {

    class BoundBatch(internal val schema: Schema, internal val recordBatch: ArrowRecordBatch) : AutoCloseable {
        override fun close() = recordBatch.close()
    }

    private sealed interface Bound : AutoCloseable {
        class InMemory(val batches: List<BoundBatch>) : Bound {
            override fun close() = batches.closeAll()
        }

        class Spilled(val spill: Spill) : Bound {
            override fun close() = spill.close()
        }
    }

    private fun spill(heldBatches: ArrayDeque<BoundBatch>): Spill =
        Relation(al, boundVecTypes).use { dataRel ->
            Spill.open(al, dataRel).closeOnCatch { spill ->
                while (heldBatches.isNotEmpty()) {
                    heldBatches.removeFirst().use { batch ->
                        Relation(al, batch.schema).use { rel ->
                            rel.load(batch.recordBatch)
                            dataRel.append(rel)
                        }
                    }
                    spill.spill()
                }

                boundCursor.forEachRemaining { rel ->
                    dataRel.append(rel)
                    spill.spill()
                }

                spill.end()
                spill
            }
        }

    private fun drain(): Bound {
        val batches = ArrayDeque<BoundBatch>()
        var rowCount = 0L

        return batches.closeAllOnCatch {
            while (rowCount <= spillThresholdRows) {
                val advanced = boundCursor.tryAdvance { rel ->
                    rowCount += rel.rowCount
                    rel.openDirectSlice(al).use { relSlice ->
                        batches += BoundBatch(relSlice.schema, relSlice.openArrowRecordBatch())
                    }
                }

                if (!advanced) return Bound.InMemory(batches)
            }

            Bound.Spilled(spill(batches))
        }
    }

    private val boundLazy: Lazy<Bound> = lazy { drain() }
    private val bound by boundLazy

    private fun openInMemory(bound: Bound.InMemory) = object : ICursor {
        private val batches = bound.batches.spliterator()

        override val cursorType get() = "relation"
        override val childCursors get() = emptyList<ICursor>()

        override fun tryAdvance(c: Consumer<in RelationReader>): Boolean =
            batches.tryAdvance { batch ->
                Relation(al, batch.schema).use { rel ->
                    rel.load(batch.recordBatch)

                    c.accept(rel)
                }
            }

        override fun estimateSize() = batches.estimateSize()
        override fun getExactSizeIfKnown() = batches.exactSizeIfKnown
        override fun characteristics() = batches.characteristics()
        override fun hasCharacteristics(characteristics: Int) = batches.hasCharacteristics(characteristics)
    }

    private fun openSpilled(bound: Bound.Spilled): ICursor =
        bound.spill.openDataLoader().closeOnCatch { loader ->
            val pages = loader.openCursor(al)

            object : ICursor {
                override val cursorType get() = "relation"
                override val childCursors get() = emptyList<ICursor>()

                override fun tryAdvance(c: Consumer<in RelationReader>) = pages.tryAdvance(c)

                override fun close() {
                    pages.close()
                    loader.close()
                }
            }
        }

    override fun open(): ICursor = when (val bound = bound) {
        is Bound.InMemory -> openInMemory(bound)
        is Bound.Spilled -> openSpilled(bound)
    }

    fun wrapBodyCursor(bodyCursor: ICursor) = object : ICursor by bodyCursor {
        override val cursorType get() = "let"
        override val childCursors get() = listOf(boundCursor, bodyCursor)

        override fun close() {
            bodyCursor.close()
            this@LetCursorFactory.close()
        }
    }

    override fun close() {
        if (boundLazy.isInitialized()) bound.close()
        boundCursor.close()
    }
}
