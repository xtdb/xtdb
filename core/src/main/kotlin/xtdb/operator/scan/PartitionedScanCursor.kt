package xtdb.operator.scan

import xtdb.api.ICursor
import xtdb.arrow.RelationReader
import xtdb.util.closeAll
import java.util.function.Consumer

/**
 * Emits each partition's [ScanCursor] back-to-back.
 *
 * Concatenation rather than a merge: partitions carry disjoint `_id`s under the upstream routing
 * contract (#5557), so there's no version of a row to resolve across them and no order key between
 * them to merge on. Each branch has already resolved its own bitemporal versions.
 *
 * Presents as a single `scan` with the branches' shared [metrics], so a partitioned scan reads in
 * EXPLAIN ANALYZE exactly as an unpartitioned one does — partition count is a physical property of
 * the database, not something the plan chose.
 */
class PartitionedScanCursor(
    private val cursors: List<ICursor>,
    private val metrics: ScanMetrics
) : ICursor {

    override val cursorType get() = "scan"
    override val childCursors get() = emptyList<ICursor>()
    override val cursorAttributes get() = metrics.toMap()

    private var idx = 0

    override fun tryAdvance(c: Consumer<in RelationReader>): Boolean {
        while (idx < cursors.size) {
            if (cursors[idx].tryAdvance(c)) return true
            idx++
        }

        return false
    }

    override fun close() = cursors.closeAll()
}
