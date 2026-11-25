package xtdb.query

import org.antlr.v4.runtime.atn.ParserATNSimulator
import org.antlr.v4.runtime.atn.PredictionContextCache
import org.antlr.v4.runtime.dfa.DFA
import xtdb.antlr.Sql
import xtdb.util.info
import xtdb.util.logger
import xtdb.util.warn
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.LongAdder

/**
 * The prediction cache the SQL parser runs on, in place of the parser's static ANTLR cache, which is never evicted.
 *
 * Once the cache holds more than [maxStates] DFA states it is replaced, whole, by an empty one;
 * parses already in flight finish on the one they started with.
 * A negative [maxStates] never replaces it.
 */
class ParserCache(private val maxStates: Int) {

    private class Generation {
        val dfa: Array<DFA> = Array(Sql._ATN.numberOfDecisions) { DFA(Sql._ATN.getDecisionState(it), it) }
        val contexts = PredictionContextCache()
        val parses = LongAdder()

        fun states() = dfa.sumOf { it.states.size }
    }

    private val generation = AtomicReference(Generation())
    private val resets = AtomicLong()

    val states: Int get() = generation.get().states()
    val resetCount: Long get() = resets.get()

    internal fun <T> parse(parser: Sql, parseAction: () -> T): T {
        val gen = generation.get()
        parser.interpreter = ParserATNSimulator(parser, Sql._ATN, gen.dfa, gen.contexts)

        try {
            return parseAction()
        } finally {
            gen.parses.increment()
            if (maxStates >= 0) {
                val states = gen.states()
                if (states > maxStates && generation.get() === gen && generation.compareAndSet(gen, Generation())) {
                    resets.incrementAndGet()
                    LOG.info(
                        "SQL parser cache reset at %,d states (est. ~%,dMB) after %,d parses; see %s"
                            .format(states, states.toLong() * BYTES_PER_STATE / (1024 * 1024), gen.parses.sum(), DOCS_URL)
                    )
                }
            }
        }
    }

    companion object {
        private val LOG = ParserCache::class.logger

        // measured at 9-11KB per state, context cache included, across SLT and nested-document corpora (#6496)
        private const val BYTES_PER_STATE = 10L * 1024

        private const val DOCS_URL = "https://docs.xtdb.com/ops/troubleshooting.html#sql-parser-cache-resets"

        internal const val DEFAULT_MAX_STATES = 50_000

        internal fun maxStates(envValue: String?, propValue: String?): Int {
            val (source, raw) = envValue?.let { "XTDB_SQL_PARSER_CACHE_MAX_STATES" to it }
                ?: propValue?.let { "xtdb.sql.parser-cache.max-states" to it }
                ?: return DEFAULT_MAX_STATES

            return raw.trim().toIntOrNull()
                ?: DEFAULT_MAX_STATES.also {
                    LOG.warn("$source='$raw' is not a whole number of states - using the default, %,d".format(it))
                }
        }

        @JvmStatic
        val global = ParserCache(
            maxStates(System.getenv("XTDB_SQL_PARSER_CACHE_MAX_STATES"), System.getProperty("xtdb.sql.parser-cache.max-states"))
        )
    }
}
