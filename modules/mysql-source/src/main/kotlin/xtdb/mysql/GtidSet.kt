package xtdb.mysql

import xtdb.api.error.Unsupported
import java.util.UUID

data class Gtid(val sourceId: UUID, val sequence: Long) {
    override fun toString() = "$sourceId:$sequence"
}

/**
 * A set of upstream transactions, in MySQL's own `uuid:1-5:7,uuid2:1-3` text form.
 *
 * As a position, it's every transaction already indexed: each transaction's position is the one before it plus
 * that transaction's GTID.
 */
class GtidSet private constructor(private val intervals: Map<UUID, List<LongRange>>) {

    val isEmpty get() = intervals.isEmpty()

    operator fun contains(gtid: Gtid): Boolean =
        intervals[gtid.sourceId]?.any { gtid.sequence in it } == true

    operator fun plus(gtid: Gtid): GtidSet =
        if (gtid in this) this
        else GtidSet(intervals + (gtid.sourceId to coalesce(intervals[gtid.sourceId].orEmpty() + listOf(gtid.sequence..gtid.sequence))))

    fun isSubsetOf(other: GtidSet): Boolean =
        intervals.all { (id, ranges) ->
            val theirs = other.intervals[id].orEmpty()
            ranges.all { r -> theirs.any { t -> r.first >= t.first && r.last <= t.last } }
        }

    override fun equals(other: Any?) = other is GtidSet && intervals == other.intervals
    override fun hashCode() = intervals.hashCode()

    override fun toString() =
        intervals.entries.sortedBy { it.key.toString() }.joinToString(",") { (id, ranges) ->
            id.toString() + ranges.joinToString("") { r -> if (r.first == r.last) ":${r.first}" else ":${r.first}-${r.last}" }
        }

    companion object {
        val EMPTY = GtidSet(emptyMap())

        private val INTERVAL = Regex("""\d+(-\d+)?""")

        /** Accepts the server's own output, including the newlines `@@gtid_executed` is formatted with. */
        fun parse(s: String?): GtidSet {
            val text = s.orEmpty().filterNot { it.isWhitespace() }
            if (text.isEmpty()) return EMPTY

            val parsed = text.split(',').map { part ->
                val pieces = part.split(':')
                require(pieces.size >= 2) { "not a GTID set: '$s'" }

                UUID.fromString(pieces[0]) to pieces.drop(1).map { p ->
                    if (!p.matches(INTERVAL))
                        throw Unsupported("tagged GTIDs aren't supported (got '$part')", "xtdb.mysql/tagged-gtid", mapOf("gtid-set" to s))

                    val bounds = p.split('-', limit = 2).map(String::toLong)
                    bounds.first()..bounds.last()
                }
            }

            return GtidSet(parsed.groupBy({ it.first }, { it.second }).mapValues { (_, lists) -> coalesce(lists.flatten()) })
        }

        private fun coalesce(ranges: List<LongRange>): List<LongRange> {
            val merged = mutableListOf<LongRange>()
            for (r in ranges.sortedBy { it.first }) {
                val last = merged.lastOrNull()
                if (last != null && r.first <= last.last + 1) merged[merged.lastIndex] = last.first..maxOf(last.last, r.last)
                else merged.add(r)
            }
            return merged
        }
    }
}
