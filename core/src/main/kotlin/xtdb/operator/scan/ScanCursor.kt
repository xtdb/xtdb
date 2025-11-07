package xtdb.operator.scan

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.ICursor
import xtdb.arrow.RelationReader
import xtdb.bitemporal.PolygonCalculator
import xtdb.operator.SelectionSpec
import xtdb.segment.MergeTask
import xtdb.segment.Segment
import xtdb.trie.ColumnName
import xtdb.trie.EventRowPointer
import xtdb.util.TemporalBounds
import xtdb.util.closeAll
import java.util.*
import java.util.Comparator.comparing
import java.util.function.Consumer

class ScanCursor(
    private val al: BufferAllocator,

    private val colNames: List<ColumnName>, private val colPreds: Map<ColumnName, SelectionSpec>,
    private val temporalBounds: TemporalBounds,

    private val segments: List<Segment<*>>,
    private val mergeTasks: Iterator<MergeTask>,

    private val schema: Map<String, Any>, private val args: RelationReader
) : ICursor {

    override val cursorType get() = "scan"
    override val childCursors get() = emptyList<ICursor>()

    private class LeafPointer(val evPtr: EventRowPointer, val relIdx: Int)

    private fun RelationReader.maybeSelect(iidPred: SelectionSpec?, path: ByteArray) =
        when (iidPred) {
            null -> this
            is MultiIidSelector -> select(iidPred.select(al, this, path))
            else -> select(iidPred.select(al, this, this@ScanCursor.schema, this@ScanCursor.args))
        }

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
        val isValidPtr = ArrowBufPointer()
        val iidPred = colPreds["_iid"]
        while (mergeTasks.hasNext()) {
            val task = mergeTasks.next()
            val taskPath = task.path
            val mergeQueue = PriorityQueue<LeafPointer>(comparing({ it.evPtr }, EventRowPointer.comparator()))
            val polygonCalculator = PolygonCalculator(temporalBounds)

            // we're not in coroutine land here, so it's a good boundary for runBlocking
            val loadedPages = runBlocking { task.pages.map { async { it.loadDataPage(al) } }.awaitAll() }

            val leafReaders = loadedPages.map { it.maybeSelect(iidPred, taskPath) }

            leafReaders.forEachIndexed { idx, leafReader ->
                val evPtr = EventRowPointer(leafReader, taskPath)
                if (!evPtr.isValid(isValidPtr, taskPath)) return@forEachIndexed
                mergeQueue.add(LeafPointer(evPtr, idx))
            }

            BitemporalConsumer.open(al, leafReaders, colNames).use { bitemporalConsumer ->
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
                                    bitemporalConsumer.accept(leafPtr.relIdx, idx, validFrom, validTo, sysFrom, sysTo)
                                }
                            }
                        }

                    evPtr.nextIndex()

                    if (evPtr.isValid(isValidPtr, taskPath)) mergeQueue.add(leafPtr)
                }

                val colPreds = colPreds.entries
                    .filterNot { it.key == "_iid" }
                    .map { it.value }

                val rel = bitemporalConsumer.build { childRel ->
                    colPreds
                        .fold(childRel) { acc, colPred -> acc.select(colPred.select(al, acc, schema, args)) }
                }

                if (rel.rowCount > 0) {
                    c.accept(rel)
                    return true
                }
            }
        }

        return false
    }

    override fun close() = segments.closeAll()
}
