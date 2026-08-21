package xtdb.operator.scan

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.ICursor
import xtdb.arrow.RelationReader
import xtdb.operator.SelectionSpec
import xtdb.segment.MergeTask
import xtdb.segment.Segment
import xtdb.trie.ColumnName
import xtdb.trie.EventRowPointer
import xtdb.util.TemporalBounds
import xtdb.util.closeAll
import xtdb.util.safeMap
import java.util.*
import java.util.function.Consumer

class ScanCursor(
    private val al: BufferAllocator,

    private val colNames: List<ColumnName>, private val colPreds: Map<ColumnName, SelectionSpec>,
    private val temporalBounds: TemporalBounds, private val clampValidTime: Boolean,

    private val segments: List<Segment<*>>,
    private val mergeTasks: Iterator<MergeTask>,

    private val schema: Map<String, Any>, private val args: RelationReader,

    private val metrics: ScanMetrics
) : ICursor {

    override val cursorType get() = "scan"
    override val childCursors get() = emptyList<ICursor>()
    override val cursorAttributes get() = metrics.toMap()

    private fun openResolver(): EntityResolver = PolygonResolver(temporalBounds, clampValidTime)

    private fun RelationReader.maybeSelect(iidPred: SelectionSpec?, path: ByteArray) =
        when (iidPred) {
            null -> this
            is MultiIidSelector -> select(iidPred.select(al, this, path))
            else -> select(iidPred.select(al, this, this@ScanCursor.schema, this@ScanCursor.args))
        }

    private val bufferedRels: Queue<RelationReader> = LinkedList()

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
        bufferedRels.poll()?.use {
            c.accept(it)
            return true
        }

        val iidPred = colPreds["_iid"]
        while (mergeTasks.hasNext()) {
            val task = mergeTasks.next()
            val taskPath = task.path
            val resolver = openResolver()

            // we're not in coroutine land here, so it's a good boundary for runBlocking
            val loadedPages = runBlocking { task.pages.map { async { it.loadDataPage(al) } }.awaitAll() }

            // rows physically read off the pages we loaded — the scan's throughput denominator,
            // before iid-selection and bitemporal resolution whittle them down to the emitted rows
            metrics.addRowsRead(loadedPages.sumOf { it.rowCount.toLong() })

            val leafReaders = loadedPages.map { it.maybeSelect(iidPred, taskPath) }

            val pointers = leafReaders.mapIndexedNotNull { idx, leafReader ->
                EventRowPointer(leafReader, taskPath).takeIf { it.isValid() }?.let { LeafPointer(it, idx) }
            }

            BitemporalConsumer.open(al, leafReaders, colNames).use { bitemporalConsumer ->
                val merge = EntityMerge(pointers)

                while (merge.nextEntity())
                    resolver.resolveEntity(merge, bitemporalConsumer)

                val colPreds = colPreds.entries
                    .filterNot { it.key == "_iid" }
                    .map { it.value }

                bitemporalConsumer.build()
                    .map { childRel ->
                        colPreds.fold(childRel) { acc, colPred -> acc.select(colPred.select(al, acc, schema, args)) }
                    }
                    .filter { it.rowCount > 0 }
                    .safeMap { it.openSlice(al) }
                    .also { bufferedRels.addAll(it) }

                bufferedRels.poll()?.use {
                    c.accept(it)
                    return true
                }
            }
        }

        return false
    }

    override fun close() {
        bufferedRels.apply { closeAll(); clear() }
        bufferedRels.clear()
        segments.closeAll()
    }
}
