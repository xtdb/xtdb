package xtdb.query

import java.time.Duration

interface ExplainAnalyze {
    val rowCount: Long
    val pageCount: Int
    val timeToFirstPage: Duration?
    val totalTime: Duration
    val pushdowns: Map<String, Any>?
    val cursorAttributes: Map<String, Any>?

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
