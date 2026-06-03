package xtdb.operator.scan

/**
 * Per-scan explain-analyze counters. Identity (db, source) is known when the scan is planned;
 * the file/page counts accrue during planning as tries and pages are pruned vs kept; rows-read
 * accrues as the cursor loads pages. Read once, after the cursor is fully consumed.
 *
 * `files`/`pages` pruned counts only ever cover metadata the planner already consulted — a pruned
 * page is one we tested and rejected — so they're free, and we never touch files we skipped.
 */
class ScanMetrics(private val scanDb: String, private val scanSource: String) {
    var filesPruned: Long = 0; private set
    var filesUsed: Long = 0; private set
    var pagesPruned: Long = 0; private set
    var pagesUsed: Long = 0; private set
    var rowsRead: Long = 0; private set

    fun addFiles(pruned: Long, used: Long) { filesPruned += pruned; filesUsed += used }
    fun addPages(pruned: Long, used: Long) { pagesPruned += pruned; pagesUsed += used }
    fun addRowsRead(rows: Long) { rowsRead += rows }

    fun toMap(): Map<String, Any> =
        mapOf(
            "scan_db" to scanDb, "scan_source" to scanSource,
            "scan_files_pruned" to filesPruned, "scan_files_used" to filesUsed,
            "scan_pages_pruned" to pagesPruned, "scan_pages_used" to pagesUsed,
            "scan_rows_read" to rowsRead,
        )
}
