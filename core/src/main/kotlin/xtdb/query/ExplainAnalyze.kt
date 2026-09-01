package xtdb.query

import java.time.Duration

interface ExplainAnalyze {
    val rowCount: Long
    val pageCount: Int
    val timeToFirstPage: Duration?
    val totalTime: Duration
    val pushdowns: Map<String, Any>?
    val cursorAttributes: ScanAttributes?

    /**
     * A scan's counters as `EXPLAIN ANALYZE` reports them, taken once the cursor is consumed.
     *
     * The key names [toMap] produces are the field names of the `attributes` struct in
     * `xtdb.query/explain-analyze-types`, so the two have to be changed together.
     */
    data class ScanAttributes(
        val db: String, val source: String,
        val filesPruned: Long, val filesUsed: Long,
        val pagesPruned: Long, val pagesUsed: Long,
        val rowsRead: Long,
    ) {
        /**
         * Totals two runs of the same scan, for a dependent sub-plan that reopens it once per input row.
         * Every counter sums; [db] and [source] name the same table either side.
         */
        operator fun plus(other: ScanAttributes) =
            ScanAttributes(
                db, source,
                filesPruned + other.filesPruned, filesUsed + other.filesUsed,
                pagesPruned + other.pagesPruned, pagesUsed + other.pagesUsed,
                rowsRead + other.rowsRead,
            )

        fun toMap(): Map<String, Any> =
            mapOf(
                "scan_db" to db, "scan_source" to source,
                "scan_files_pruned" to filesPruned, "scan_files_used" to filesUsed,
                "scan_pages_pruned" to pagesPruned, "scan_pages_used" to pagesUsed,
                "scan_rows_read" to rowsRead,
            )
    }

    /**
     * One operator in the tree `EXPLAIN ANALYZE` reports, for a sub-plan whose cursors have already been closed
     * by the time the walk runs.
     *
     * @see xtdb.api.ICursor.extraExplainNodes
     */
    interface Node {
        val cursorType: String
        val explainAnalyze: ExplainAnalyze
        val children: List<Node>
    }
}
