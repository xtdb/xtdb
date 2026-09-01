package xtdb.operator.apply

import org.roaringbitmap.buffer.MutableRoaringBitmap
import xtdb.api.ICursor
import xtdb.query.ExplainAnalyze
import java.time.Duration

/**
 * The cursor the explain walk would report for [this], which is [this] once it carries counters, and otherwise
 * whichever descendant first does — mirroring the walk's own rule that an untraced cursor is not an operator in
 * the output and its first child takes its place.
 */
internal val ICursor.reportingCursor: ICursor?
    get() = if (explainAnalyze != null) this else childCursors.firstOrNull()?.reportingCursor

/**
 * Copies the mutable values a pushdown map holds. A join clears its pushdown blooms as it closes, which for a
 * dependent sub-plan is long before the walk reads their cardinality, so a retained reference reports zero.
 */
private fun Any?.snapshot(): Any? = when (this) {
    is MutableRoaringBitmap -> clone()
    is Map<*, *> -> mapValues { (_, v) -> v.snapshot() }
    is Collection<*> -> toList()
    else -> this
}

/**
 * `EXPLAIN ANALYZE` counters for one operator of a sub-plan that is opened, run and closed once per input row,
 * accumulated across every one of those runs so that the totals outlive the cursors they were read from.
 *
 * [rowCount], [pageCount] and [totalTime] sum. [timeToFirstPage], [pushdowns] and [cursorAttributes] keep the
 * first run's, having no meaningful sum.
 */
internal class DependentPlanReport(override val cursorType: String) : ExplainAnalyze.Node, ExplainAnalyze {

    private val childReports = mutableListOf<DependentPlanReport>()
    private val extraReports = mutableListOf<DependentPlanReport>()

    override val explainAnalyze get() = this
    override val children: List<ExplainAnalyze.Node> get() = childReports + extraReports

    override var rowCount = 0L; private set
    override var pageCount = 0; private set
    override var totalTime: Duration = Duration.ZERO; private set
    override var timeToFirstPage: Duration? = null; private set
    override var pushdowns: Map<String, Any>? = null; private set
    override var cursorAttributes: Map<String, Any>? = null; private set

    /**
     * Folds one run of the sub-plan in. [cursor] MUST be a [reportingCursor], and MUST NOT have been closed yet —
     * its counters and attributes read through to state that closing finalises.
     */
    fun accumulate(cursor: ICursor) {
        foldCounters(cursor.explainAnalyze ?: return)

        cursor.childCursors.mapNotNull { it.reportingCursor }.forEachIndexed { idx, child ->
            childReports.reportAt(idx, child.cursorType).accumulate(child)
        }

        cursor.extraExplainNodes.forEachIndexed { idx, node ->
            extraReports.reportAt(idx, node.cursorType).accumulate(node)
        }
    }

    /** Folds in a sub-plan that had already been reduced to a report — a correlated subquery nested in this one. */
    fun accumulate(node: ExplainAnalyze.Node) {
        foldCounters(node.explainAnalyze)

        node.children.forEachIndexed { idx, child ->
            childReports.reportAt(idx, child.cursorType).accumulate(child)
        }
    }

    private fun foldCounters(ea: ExplainAnalyze) {
        rowCount += ea.rowCount
        pageCount += ea.pageCount
        totalTime += ea.totalTime
        timeToFirstPage = timeToFirstPage ?: ea.timeToFirstPage
        cursorAttributes = cursorAttributes ?: ea.cursorAttributes

        if (pushdowns == null) {
            @Suppress("UNCHECKED_CAST")
            pushdowns = ea.pushdowns?.snapshot() as Map<String, Any>?
        }
    }

    private fun MutableList<DependentPlanReport>.reportAt(idx: Int, cursorType: String) =
        getOrNull(idx) ?: DependentPlanReport(cursorType).also { add(it) }
}
