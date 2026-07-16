package xtdb.query

import java.time.Duration

interface ExplainAnalyze {
    val rowCount: Long
    val pageCount: Int
    val timeToFirstPage: Duration?
    val totalTime: Duration
    val pushdowns: Map<String, Any>?
    val cursorAttributes: Map<String, Any>?
}
