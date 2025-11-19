package xtdb.query

import org.slf4j.LoggerFactory
import xtdb.antlr.Sql
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * ANTLR parser profiling utilities for performance analysis.
 *
 * Enable profiling with system property:
 *   -Dxtdb.sql.parser.profile=true
 *
 * Usage from REPL:
 *   (require '[xtdb.antlr :as antlr])
 *   ;; Run queries...
 *   (antlr/dump-aggregated-profiling)
 *   (antlr/reset-profiling-stats)
 */
internal object SqlParserProfiler {
    private val log = LoggerFactory.getLogger("xtdb.query.SqlParser")

    private val ENABLED = System.getProperty("xtdb.sql.parser.profile", "false").toBoolean()

    // Aggregated profiling data across all queries
    private class AggregatedDecisionInfo {
        val totalTime = AtomicLong(0)
        val invocations = AtomicLong(0)
        val sllLookahead = AtomicLong(0)
        val llLookahead = AtomicLong(0)
        val llAtnTransitions = AtomicLong(0)
        val llDfaTransitions = AtomicLong(0)
        @Volatile var maxLookahead = 0L
    }

    private val aggregatedStats = ConcurrentHashMap<String, AggregatedDecisionInfo>()
    private val totalQueriesParsed = AtomicLong(0)

    /**
     * Returns true if profiling is enabled via system property.
     */
    fun isEnabled() = ENABLED

    /**
     * Accumulate profiling statistics from a parser.
     * No-op if profiling is disabled.
     */
    fun accumulate(parser: Sql) {
        if (!ENABLED) return

        totalQueriesParsed.incrementAndGet()

        parser.parseInfo.decisionInfo.forEachIndexed { index, di ->
            val ds = parser.atn.decisionToState[index]
            val rule = parser.ruleNames[ds.ruleIndex]
            val key = "$index:$rule"

            val stats = aggregatedStats.computeIfAbsent(key) { AggregatedDecisionInfo() }
            stats.totalTime.addAndGet(di.timeInPrediction)
            stats.invocations.addAndGet(di.invocations)
            stats.sllLookahead.addAndGet(di.SLL_TotalLook)
            stats.llLookahead.addAndGet(di.LL_TotalLook)
            stats.llAtnTransitions.addAndGet(di.LL_ATNTransitions)
            stats.llDfaTransitions.addAndGet(di.LL_DFATransitions)
            synchronized(stats) {
                stats.maxLookahead = maxOf(stats.maxLookahead, di.LL_MaxLook)
            }
        }
    }

    /**
     * Dump aggregated ANTLR profiling statistics to the log.
     * Call this after running a batch of queries to see which parser decisions are bottlenecks.
     */
    fun dump() {
        if (!ENABLED) {
            log.warn("ANTLR profiling is not enabled. Set -Dxtdb.sql.parser.profile=true to enable.")
            return
        }

        log.info("=== ANTLR Parser Profiling Summary ===")
        log.info("Total queries parsed: ${totalQueriesParsed.get()}")
        log.info("")
        log.info("Top 30 decisions by total time:")

        aggregatedStats.entries
            .sortedByDescending { it.value.totalTime.get() }
            .take(30)
            .forEach { (key, stats) ->
                val avgTime = stats.totalTime.get() / stats.invocations.get().coerceAtLeast(1)
                log.info(
                    "$key: total=${stats.totalTime.get() / 1_000_000}ms, " +
                            "avg=${avgTime / 1000}µs, " +
                            "invocations=${stats.invocations.get()}, " +
                            "SLL_k=${stats.sllLookahead.get()}, " +
                            "LL_k=${stats.llLookahead.get()}, " +
                            "LL_ATN=${stats.llAtnTransitions.get()}, " +
                            "LL_DFA=${stats.llDfaTransitions.get()}, " +
                            "maxK=${stats.maxLookahead}"
                )
            }
    }

    /**
     * Reset all profiling statistics. Useful for focusing on specific query sets.
     */
    fun reset() {
        aggregatedStats.clear()
        totalQueriesParsed.set(0)
        log.info("ANTLR profiling statistics reset")
    }
}
