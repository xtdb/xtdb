package xtdb.compactor

import clojure.lang.Keyword
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.TaggedValue
import xtdb.api.TableRef
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.STRUCT_TYPE
import xtdb.compactor.SegmentMerge.RecencyPartitioning
import xtdb.indexer.LiveTable
import xtdb.segment.MemorySegment
import xtdb.table.TableSlug
import xtdb.time.InstantUtil.asMicros
import xtdb.time.microsAsInstant
import xtdb.trie.Trie
import xtdb.util.RowCounter
import xtdb.util.asIid
import xtdb.util.safeMap
import xtdb.util.useAll
import java.nio.ByteBuffer
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

class SegmentMergeTest {

    private val foo = TableRef("public", "foo")
    private val utc = ZoneId.of("UTC")

    private lateinit var al: BufferAllocator

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
    }

    @AfterEach
    fun tearDown() {
        al.close()
    }

    private fun year(year: Int): ZonedDateTime = ZonedDateTime.of(year, 1, 1, 0, 0, 0, 0, utc)

    private val endOfTime: ZonedDateTime = Long.MAX_VALUE.microsAsInstant.atZone(utc)

    private fun LiveTable.indexTx(systemTime: Instant, vararg docs: Pair<String, Long>): LiveTable =
        Trie.openLogDataWriter(al).use { rel ->
            val systemFrom = systemTime.asMicros
            for ((id, v) in docs) {
                rel["_iid"].writeBytes(ByteBuffer.wrap(id.asIid))
                rel["_system_from"].writeLong(systemFrom)
                rel["_valid_from"].writeLong(systemFrom)
                rel["_valid_to"].writeLong(Long.MAX_VALUE)
                rel["op"].vectorFor("put", STRUCT_TYPE, false).writeObject(mapOf("_id" to id, "v" to v))
                rel.endRow()
            }
            importData(rel)
        }

    private fun LiveTable.indexTxEvents(systemTime: Instant, vararg events: Triple<Long, Int, Int>): LiveTable =
        Trie.openLogDataWriter(al).use { rel ->
            val systemFrom = systemTime.asMicros
            for ((v, validFrom, validTo) in events) {
                rel["_iid"].writeBytes(ByteBuffer.wrap("foo".asIid))
                rel["_system_from"].writeLong(systemFrom)
                rel["_valid_from"].writeLong(year(validFrom).toInstant().asMicros)
                rel["_valid_to"].writeLong(year(validTo).toInstant().asMicros)
                rel["op"].vectorFor("put", STRUCT_TYPE, false).writeObject(mapOf("_id" to "foo", "v" to v))
                rel.endRow()
            }
            importData(rel)
        }

    private fun put(id: String, v: Long, year: Int) = mapOf(
        "_iid" to ByteBuffer.wrap(id.asIid),
        "_system_from" to year(year),
        "_valid_from" to year(year),
        "_valid_to" to endOfTime,
        "op" to TaggedValue(Keyword.intern("put"), mapOf("_id" to id, "v" to v))
    )

    private fun SegmentMerge.rowsOf(result: SegmentMerge.Result) =
        result.openAllAsRelation().use { rel ->
            rel.toMaps(SNAKE_CASE_STRING).map { row ->
                row.mapValues { (k, v) -> if (k == "_iid") ByteBuffer.wrap(v as ByteArray) else v }
            }
        }

    private fun withSegments(block: (List<MemorySegment>) -> Unit) {
        LiveTable.open(al, foo, TableSlug.of(foo), 0L, RowCounter()).use { lt0Base ->
            LiveTable.open(al, foo, TableSlug.of(foo), 0L, RowCounter()).use { lt1Base ->
                val lt0 = lt0Base
                    .indexTx(Instant.parse("2020-01-01T00:00:00Z"), "foo" to 0L, "bar" to 0L)
                    .indexTx(Instant.parse("2021-01-01T00:00:00Z"), "bar" to 1L)

                val lt1 = lt1Base
                    .indexTx(Instant.parse("2022-01-01T00:00:00Z"), "foo" to 1L)
                    .indexTx(Instant.parse("2023-01-01T00:00:00Z"), "foo" to 2L, "bar" to 2L)

                listOf(lt0, lt1)
                    .safeMap { lt ->
                        val rel = lt.relation.openSlice(al)
                        MemorySegment(lt.trie.compactLogs().withIidReader(rel["_iid"]), rel)
                    }
                    .useAll(block)
            }
        }
    }

    @Test
    fun `merging segments yields each iid's events newest first`() = withSegments { segments ->
        SegmentMerge(al).use { segMerge ->
            segMerge.mergeSegmentsSync(segments, null, RecencyPartitioning.Preserve(null)).use { results ->
                assertEquals(
                    listOf(
                        listOf(
                            put("bar", 2, 2023), put("bar", 1, 2021), put("bar", 0, 2020),
                            put("foo", 2, 2023), put("foo", 1, 2022), put("foo", 0, 2020)
                        )
                    ),
                    results.map { segMerge.rowsOf(it) }
                )
            }
        }
    }

    @Test
    fun `a path filter restricts the merge to the iids under that path`() = withSegments { segments ->
        SegmentMerge(al).use { segMerge ->
            segMerge.mergeSegmentsSync(segments, byteArrayOf(2), RecencyPartitioning.Preserve(null)).use { results ->
                assertEquals(
                    listOf(listOf(put("bar", 2, 2023), put("bar", 1, 2021), put("bar", 0, 2020))),
                    results.map { segMerge.rowsOf(it) }
                )
            }
        }
    }

    @Test
    fun `partitioning by recency splits superseded events into weekly files`() = withSegments { segments ->
        SegmentMerge(al).use { segMerge ->
            segMerge.mergeSegmentsSync(segments, null, RecencyPartitioning.Partition).use { results ->
                assertEquals(
                    mapOf(
                        "r20210104.arrow" to listOf(put("bar", 0, 2020)),
                        "r20220103.arrow" to listOf(put("foo", 0, 2020)),
                        "r20230102.arrow" to listOf(put("bar", 1, 2021), put("foo", 1, 2022)),
                        "rc.arrow" to listOf(put("bar", 2, 2023), put("foo", 2, 2023)),
                    ),
                    results.associate { it.path.fileName.toString() to segMerge.rowsOf(it) }
                )
            }
        }
    }

    @Test
    fun `a same-transaction event overridden by a later one is resolved away before partitioning by recency`() {
        LiveTable.open(al, foo, TableSlug.of(foo), 0L, RowCounter()).use { liveTable ->
            val lt = liveTable.indexTxEvents(
                Instant.parse("2020-01-01T00:00:00Z"),
                Triple(1L, 2023, 2024), Triple(2L, 2022, 2025)
            )

            val rel = lt.relation.openSlice(al)
            MemorySegment(lt.trie.compactLogs().withIidReader(rel["_iid"]), rel).use { segment ->
                SegmentMerge(al).use { segMerge ->
                    segMerge.mergeSegmentsSync(listOf(segment), null, RecencyPartitioning.Partition).use { results ->
                        assertEquals(
                            mapOf(
                                "r20250106.arrow" to listOf(
                                    put("foo", 2, 2020) + mapOf("_valid_from" to year(2022), "_valid_to" to year(2025))
                                ),
                                "rc.arrow" to emptyList()
                            ),
                            results.associate { it.path.fileName.toString() to segMerge.rowsOf(it) }
                        )
                    }
                }
            }
        }
    }
}
