package xtdb.compactor

import clojure.lang.Keyword
import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.TaggedValue
import xtdb.api.TableRef
import xtdb.api.query.IKeyFn.KeyFn.SNAKE_CASE_STRING
import xtdb.arrow.Relation
import xtdb.arrow.RunEndEncodedVector
import xtdb.arrow.STRUCT_TYPE
import xtdb.compactor.PageTree.Companion.asTree
import xtdb.compactor.SegmentMerge.RecencyPartitioning
import xtdb.indexer.LiveTable
import xtdb.segment.MemorySegment
import xtdb.storage.MemoryStorage
import xtdb.table.TableSlug
import xtdb.time.InstantUtil.asMicros
import xtdb.time.microsAsInstant
import xtdb.trie.PageTrieWriter
import xtdb.trie.Trie
import xtdb.util.RowCounter
import xtdb.util.asIid
import xtdb.util.safeMap
import xtdb.util.useAll
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
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

    private fun SegmentMerge.iidRunEndsOf(result: SegmentMerge.Result) =
        result.openAllAsRelation().use { rel ->
            (rel["_iid"] as? RunEndEncodedVector)?.let { iid -> (0..<iid.runCount).map { iid.runEnd(it) } }
        }

    private val historical = RecencyPartitioning.Preserve(LocalDate.of(2023, 1, 2))

    @Test
    fun `with the iid run-end encoded, a historical merge writes the same rows as one run per entity`() = withSegments { segments ->
        SegmentMerge(al).use { plain ->
            SegmentMerge(al, reeIids = true).use { encoded ->
                plain.mergeSegmentsSync(segments, null, historical).use { plainResults ->
                    encoded.mergeSegmentsSync(segments, null, historical).use { encodedResults ->
                        assertEquals(plainResults.map { plain.rowsOf(it) }, encodedResults.map { encoded.rowsOf(it) })
                        assertEquals(listOf(listOf(3, 6)), encodedResults.map { encoded.iidRunEndsOf(it) })
                    }
                }
            }
        }
    }

    @Test
    fun `with the iid run-end encoded, a current-tier merge still stores its iids plain`() = withSegments { segments ->
        SegmentMerge(al, reeIids = true).use { encoded ->
            encoded.mergeSegmentsSync(segments, null, RecencyPartitioning.Preserve(null)).use { results ->
                assertEquals(listOf(null), results.map { encoded.iidRunEndsOf(it) })
            }
        }
    }

    @Test
    fun `with the iid run-end encoded, each weekly file holds its own runs and the current file stays plain`() = withSegments { segments ->
        SegmentMerge(al).use { plain ->
            SegmentMerge(al, reeIids = true).use { encoded ->
                plain.mergeSegmentsSync(segments, null, RecencyPartitioning.Partition).use { plainResults ->
                    encoded.mergeSegmentsSync(segments, null, RecencyPartitioning.Partition).use { encodedResults ->
                        assertEquals(
                            plainResults.associate { it.path.fileName.toString() to plain.rowsOf(it) },
                            encodedResults.associate { it.path.fileName.toString() to encoded.rowsOf(it) }
                        )

                        assertEquals(
                            mapOf(
                                "r20210104.arrow" to listOf(1),
                                "r20220103.arrow" to listOf(1),
                                "r20230102.arrow" to listOf(1, 2),
                                "rc.arrow" to null
                            ),
                            encodedResults.associate { it.path.fileName.toString() to encoded.iidRunEndsOf(it) }
                        )
                    }
                }
            }
        }
    }

    @Test
    fun `a trie written with the iid run-end encoded has the same metadata and pages as a plain one`() =
        withSegments { segments ->
            MemoryStorage(al, epoch = 0).use { bp ->
                val slug = TableSlug.of(foo)
                val trieWriter = PageTrieWriter(al, bp, calculateBlooms = true)

                fun writeTrie(reeIids: Boolean, trieKey: String, pageSize: Int) =
                    SegmentMerge(al, reeIids).use { segMerge ->
                        segMerge.mergeSegmentsSync(segments, null, historical).use { results ->
                            val result = results.single()
                            Relation.loader(al, result.openForRead()).use { loader ->
                                runBlocking { trieWriter.writePageTree(slug, trieKey, loader, result.leaves.asTree, pageSize) }
                            }
                        }
                    }

                fun pagesOf(trieKey: String) =
                    Relation.loader(al, bp.getByteArray(slug.dataFilePath(trieKey))).use { loader ->
                        Relation(al, loader.schema).use { rel ->
                            buildList {
                                while (loader.loadNextPage(rel))
                                    add(rel.toMaps(SNAKE_CASE_STRING).map { row -> row.mapValues { (_, v) -> if (v is ByteArray) ByteBuffer.wrap(v) else v } })
                            }
                        }
                    }

                fun iidRunCountsOf(trieKey: String) =
                    Relation.loader(al, bp.getByteArray(slug.dataFilePath(trieKey))).use { loader ->
                        Relation(al, loader.schema).use { rel ->
                            buildList { while (loader.loadNextPage(rel)) add((rel["_iid"] as RunEndEncodedVector).runCount) }
                        }
                    }

                for ((pageSize, expectedRunCounts) in listOf(1024 to listOf(2), 2 to listOf(1, 1))) {
                    writeTrie(false, "l01-r20230102-b00", pageSize)
                    writeTrie(true, "l01-r20230102-b01", pageSize)

                    assertArrayEquals(
                        bp.getByteArray(slug.metaFilePath("l01-r20230102-b00")),
                        bp.getByteArray(slug.metaFilePath("l01-r20230102-b01")),
                        "meta files, page size $pageSize"
                    )

                    assertEquals(pagesOf("l01-r20230102-b00"), pagesOf("l01-r20230102-b01"), "pages, page size $pageSize")
                    assertEquals(expectedRunCounts, iidRunCountsOf("l01-r20230102-b01"), "runs per page, page size $pageSize")
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
