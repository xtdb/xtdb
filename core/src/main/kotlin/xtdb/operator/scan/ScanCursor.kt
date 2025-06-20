package xtdb.operator.scan

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.ICursor
import xtdb.bitemporal.PolygonCalculator
import xtdb.operator.SelectionSpec
import xtdb.time.TEMPORAL_COL_NAMES
import xtdb.trie.ColumnName
import xtdb.trie.EventRowPointer
import xtdb.util.TemporalBounds
import xtdb.vector.MultiVectorRelationFactory
import xtdb.arrow.RelationReader
import xtdb.vector.RelationWriter
import java.util.*
import java.util.Comparator.comparing
import java.util.function.Consumer

class ScanCursor(
    private val al: BufferAllocator, private val rootCache: RootCache,

    private val colNames: Set<ColumnName>, private val colPreds: Map<ColumnName, SelectionSpec>,
    private val temporalBounds: TemporalBounds,

    private val mergeTasks: Iterator<MergeTask>,

    private val schema: Map<String, Any>, private val args: RelationReader,
) : ICursor<RelationReader> {

    class MergeTask(val pages: List<MergePlanPage>, val path: ByteArray)

    private class LeafPointer(val evPtr: EventRowPointer, val relIdx: Int)

    private fun RelationReader.maybeSelect(iidPred: SelectionSpec?) =
        if (iidPred != null) select(iidPred.select(al, this, this@ScanCursor.schema, args)) else this

    override fun tryAdvance(action: Consumer<in RelationReader>): Boolean {
        val isValidPtr = ArrowBufPointer()
        val iidPred = colPreds["_iid"]

        while (mergeTasks.hasNext()) {
            rootCache.reset()

            val task = mergeTasks.next()
            val taskPath = task.path
            val mergeQueue = PriorityQueue<LeafPointer>(comparing({ it.evPtr }, EventRowPointer.comparator()))
            val polygonCalculator = PolygonCalculator(temporalBounds)

            RelationWriter(al).use { outRel ->
                val bitemporalConsumer = BitemporalConsumer(outRel, colNames)
                val leafReaders = task.pages.map { it.loadPage(rootCache).maybeSelect(iidPred) }

                val (temporalCols, contentCols) = colNames.groupBy { it in TEMPORAL_COL_NAMES }
                    .let { it[true] to it[false] }

                val contentRelFactory = MultiVectorRelationFactory(leafReaders, colNames.toList())

                leafReaders.forEachIndexed { idx, leafReader ->
                    EventRowPointer(leafReader, taskPath)
                        .takeIf { it.isValid(isValidPtr, taskPath) }
                        ?.let { mergeQueue.add(LeafPointer(it, idx)) }
                }

                while (true) {
                    val leafPtr = mergeQueue.poll() ?: break
                    val evPtr = leafPtr.evPtr

                    polygonCalculator.calculate(evPtr)
                        ?.takeIf { evPtr.op == "put" }
                        ?.let { polygon ->
                            val sysFrom = evPtr.systemFrom
                            val idx = evPtr.index

                            repeat(polygon.validTimeRangeCount) { i ->
                                val validFrom = polygon.getValidFrom(i)
                                val validTo = polygon.getValidTo(i)
                                val sysTo = polygon.getSystemTo(i)

                                if (
                                    temporalBounds.intersects(validFrom, validTo, sysFrom, sysTo)
                                    && validFrom != validTo && sysFrom != sysTo
                                ) {
                                    contentRelFactory.accept(leafPtr.relIdx, idx)
                                    bitemporalConsumer.accept(validFrom, validTo, sysFrom, sysTo)
                                    outRel.endRow()
                                }
                            }
                        }

                    evPtr.nextIndex()

                    if (evPtr.isValid(isValidPtr, taskPath)) mergeQueue.add(leafPtr)
                }

                val rel = contentRelFactory.realize()
                    .let { rel ->
                        if (contentCols.isNullOrEmpty() || !temporalCols.isNullOrEmpty())
                            RelationReader.concatCols(rel, outRel.toReader())
                        else rel
                    }
                    .let { rel ->
                        colPreds.entries.asSequence()
                            .filterNot { it.key == "_iid" }
                            .map { it.value }
                            .fold(rel) { acc, colPred -> acc.select(colPred.select(al, acc, schema, args)) }
                    }

                if (rel.rowCount > 0) {
                    action.accept(rel)
                    return true
                }
            }
        }

        return false
    }

    override fun close() {
        rootCache.close()
    }
}
