package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.ICursor
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.util.closeAll
import xtdb.util.closeAllOnCatch
import java.util.function.Consumer

class LetCursorFactory(
    private val al: BufferAllocator, private val boundCursor: ICursor<RelationReader>,
) : ICursor.Factory<RelationReader>, AutoCloseable {

    class BoundBatch(internal val schema: Schema, internal val recordBatch: ArrowRecordBatch) : AutoCloseable {
        override fun close() = recordBatch.close()
    }

    private val boundBatchesLazy: Lazy<List<BoundBatch>> = lazy {
        mutableListOf<BoundBatch>().closeAllOnCatch { boundBatches ->
            boundCursor.forEachRemaining { rel ->
                rel.openDirectSlice(al).use { relSlice ->
                    boundBatches += BoundBatch(relSlice.schema, relSlice.openArrowRecordBatch())
                }
            }

            boundBatches
        }
    }

    private val boundBatches by boundBatchesLazy

    override fun open() = object : ICursor<RelationReader> {
        private val batches = boundBatches.spliterator()

        override fun tryAdvance(c: Consumer<in RelationReader>): Boolean =
            batches.tryAdvance { batch ->
                Relation(al, batch.schema).use { rel ->
                    rel.load(batch.recordBatch)

                    // TODO: don't need all this openAsRoot dance when the operators all use xtdb.arrow
                    rel.openAsRoot(al).use { root ->
                        c.accept(RelationReader.Companion.from(root))
                    }
                }
            }

        override fun estimateSize() = batches.estimateSize()
        override fun getExactSizeIfKnown() = batches.exactSizeIfKnown
        override fun characteristics() = batches.characteristics()
        override fun hasCharacteristics(characteristics: Int) = batches.hasCharacteristics(characteristics)
    }

    fun wrapBodyCursor(bodyCursor: ICursor<RelationReader>) = object : ICursor<RelationReader> by bodyCursor {
        override fun close() {
            bodyCursor.close()
            this@LetCursorFactory.close()
        }
    }

    override fun close() {
        if (boundBatchesLazy.isInitialized()) boundBatches.closeAll()
        boundCursor.close()
    }
}