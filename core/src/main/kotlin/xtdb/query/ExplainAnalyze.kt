package xtdb.query

import io.micrometer.tracing.Span
import java.time.Duration

interface ExplainAnalyze {
    val rowCount: Long
    val pageCount: Int
    val timeToFirstPage: Duration?
    /** Time in this cursor's `tryAdvance`, including its subtree but excluding the consumer's `accept`. */
    val totalTime: Duration
    /** Peak memory this operator itself allocated, excluding the operators below it. */
    val peakMemoryBytes: Long
    val pushdowns: Map<String, Any>?
    val cursorAttributes: ScanAttributes?

    /**
     * A scan's counters as `EXPLAIN ANALYZE` reports them, taken once the cursor is consumed.
     *
     * The key names [toMap] produces, and [tag] tags a span with, are the field names of the
     * `attributes` struct in `xtdb.query/explain-analyze-types`, so the three have to be changed together.
     */
    data class ScanAttributes(
        val db: String, val source: String,
        val filesPruned: Long, val filesUsed: Long,
        val pagesPruned: Long, val pagesUsed: Long,
        val rowsRead: Long,
    ) {
        fun toMap(): Map<String, Any> =
            mapOf(
                "scan_db" to db, "scan_source" to source,
                "scan_files_pruned" to filesPruned, "scan_files_used" to filesUsed,
                "scan_pages_pruned" to pagesPruned, "scan_pages_used" to pagesUsed,
                "scan_rows_read" to rowsRead,
            )

        fun tag(span: Span) {
            span.tag("scan_db", db)
            span.tag("scan_source", source)
            span.tag("scan_files_pruned", filesPruned)
            span.tag("scan_files_used", filesUsed)
            span.tag("scan_pages_pruned", pagesPruned)
            span.tag("scan_pages_used", pagesUsed)
            span.tag("scan_rows_read", rowsRead)
        }
    }
}
