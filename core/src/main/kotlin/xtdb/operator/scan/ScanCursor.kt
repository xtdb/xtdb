package xtdb.operator.scan

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import xtdb.api.ICursor
import xtdb.arrow.Relation
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

private val SCAN_DEBUG = java.lang.Boolean.getBoolean("xtdb.scan.debug")

internal object ScanStats {
    class Hist { val n = java.util.concurrent.atomic.AtomicLong(); val sum = java.util.concurrent.atomic.AtomicLong(); val buckets = java.util.concurrent.ConcurrentHashMap<Int, java.util.concurrent.atomic.AtomicLong>()
        fun add(v: Int) { n.incrementAndGet(); sum.addAndGet(v.toLong()); val b = if (v == 0) 0 else 32 - Integer.numberOfLeadingZeros(v); buckets.computeIfAbsent(b) { java.util.concurrent.atomic.AtomicLong() }.incrementAndGet() }
        override fun toString() = "n=$n mean=${if (n.get() == 0L) 0 else sum.get() / n.get()} log2hist=" + buckets.entries.sortedBy { it.key }.joinToString(" ") { "<${1 shl it.key}:${it.value}" }
    }
    val hists = java.util.concurrent.ConcurrentHashMap<String, Hist>()
    fun add(k: String, v: Int) = hists.computeIfAbsent(k) { Hist() }.add(v)
    init {
        if (SCAN_DEBUG) Runtime.getRuntime().addShutdownHook(Thread {
            hists.entries.sortedBy { it.key }.forEach { (k, h) -> System.err.println("[scan] $k $h") }
        })
    }
}

private val COALESCE = System.getProperty("xtdb.scan.coalesce", "none")
private val COALESCE_TARGET = Integer.getInteger("xtdb.scan.coalesceTarget", 1024)

private val COMPOSE_SELECTIONS = System.getProperty("xtdb.composeSelections", "true").toBoolean()

class ScanCursor(
    private val al: BufferAllocator,

    private val colNames: List<ColumnName>, private val colPreds: Map<ColumnName, SelectionSpec>,
    private val temporalBounds: TemporalBounds, private val clampValidTime: Boolean,

    private val ownedSegments: List<Segment<*>>,
    private val mergeTasks: Iterator<MergeTask>,

    private val schema: Map<String, Any>, private val args: RelationReader,

    private val metrics: ScanMetrics
) : ICursor {

    override val cursorType get() = "scan"
    override val childCursors get() = emptyList<ICursor>()
    override val cursorAttributes get() = metrics.snapshot()

    private fun openResolver(): EntityResolver =
        if (temporalBounds.validTime.isPoint && temporalBounds.systemTime.isPoint)
            AsOfResolver(temporalBounds, clampValidTime)
        else PolygonResolver(temporalBounds, clampValidTime)

    private fun RelationReader.maybeSelect(iidPred: SelectionSpec?, path: ByteArray) =
        when (iidPred) {
            null -> this
            is MultiIidSelector -> select(iidPred.select(al, this, path))
            else -> select(iidPred.select(al, this, this@ScanCursor.schema, this@ScanCursor.args))
        }

    private val bufferedRels: Queue<RelationReader> = LinkedList()
    private var pending: Relation? = null

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
            val dbg = SCAN_DEBUG && "l_shipdate" in colNames
            if (dbg) {
                ScanStats.add("tasks.pagesPerTask", task.pages.size)
                ScanStats.add("tasks.pathDepth", taskPath.size)
                task.pages.zip(loadedPages).forEach { (pg, rel) -> ScanStats.add("page.rows.${pg.debugLabel}", rel.rowCount) }
                ScanStats.add("tasks.levels.${task.pages.map { it.debugLabel }.sorted().joinToString("+")}", 1)
            }

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

                val built = bitemporalConsumer.build()
                if (dbg) { ScanStats.add("emit.relsPerTask", built.size); built.forEach { r -> ScanStats.add("emit.rowsPreSelect", r.rowCount) } }

                val pre = if (COALESCE == "pre" && built.size > 1) listOf(concatRels(built)) else built
                try {
                    val colPreds = colPreds.entries
                        .filterNot { it.key == "_iid" }
                        .map { it.value }

                    val selected = pre
                        .map { childRel ->
                            if (COMPOSE_SELECTIONS) {
                                var sel: IntArray? = null
                                for (colPred in colPreds) {
                                    sel = colPred.selectWithin(al, childRel, sel, schema, args)
                                    if (sel.isEmpty()) break
                                }
                                sel?.let { childRel.select(it) } ?: childRel
                            } else
                                colPreds.fold(childRel) { acc, colPred -> acc.select(colPred.select(al, acc, schema, args)) }
                        }
                        .also { if (dbg) it.forEach { r -> ScanStats.add("emit.rowsPostSelect", r.rowCount) } }
                        .filter { it.rowCount > 0 }

                    if (COALESCE == "post") {
                        for (r in selected) {
                            val out = pending ?: Relation(al).also { pending = it }
                            r.rowCopier(out).copyRange(0, r.rowCount)
                            if (out.rowCount >= COALESCE_TARGET) {
                                pending = null
                                if (dbg) ScanStats.add("emit.coalescedRows", out.rowCount)
                                bufferedRels.add(out)
                            }
                        }
                    } else {
                        if (dbg) selected.forEach { ScanStats.add("emit.coalescedRows", it.rowCount) }
                        selected.safeMap { it.openSlice(al) }.also { bufferedRels.addAll(it) }
                    }
                } finally {
                    if (pre !== built) pre.forEach { it.close() }
                }

                bufferedRels.poll()?.use {
                    c.accept(it)
                    return true
                }
            }
        }

        pending?.let { p ->
            pending = null
            if (SCAN_DEBUG && "l_shipdate" in colNames) ScanStats.add("emit.coalescedRows", p.rowCount)
            p.use { c.accept(it) }
            return true
        }

        return false
    }

    private fun concatRels(rels: List<RelationReader>): Relation {
        val out = Relation(al)
        try {
            for (r in rels) r.rowCopier(out).copyRange(0, r.rowCount)
        } catch (e: Throwable) {
            out.close(); throw e
        }
        return out
    }

    override fun close() {
        pending?.close(); pending = null
        bufferedRels.apply { closeAll(); clear() }
        bufferedRels.clear()
        ownedSegments.closeAll()
    }
}
