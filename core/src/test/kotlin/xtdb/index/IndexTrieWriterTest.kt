package xtdb.index

import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.arrow.Relation
import xtdb.arrow.STRUCT_TYPE
import xtdb.arrow.Vector
import xtdb.storage.MemoryStorage
import xtdb.table.IndexFiles
import xtdb.table.TableSlug
import xtdb.trie.ArrowHashTrie
import xtdb.trie.HashTrie
import xtdb.trie.Trie
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.UUID

class IndexTrieWriterTest {
    private lateinit var al: BufferAllocator
    private lateinit var bp: MemoryStorage

    private val files = IndexFiles(TableSlug("public\$docs"), UUID.fromString("00000000-0000-0000-0000-00000000000a"))

    @BeforeEach
    fun setUp() {
        al = RootAllocator()
        bp = MemoryStorage(al, epoch = 0)
    }

    @AfterEach
    fun tearDown() {
        bp.close()
        al.close()
    }

    private fun iid(n: Int) = ByteArray(16).also { it[15] = n.toByte(); it[14] = (n shr 8).toByte() }

    private fun Relation.put(iid: ByteArray, doc: Map<String, Any?>) {
        this["_iid"].writeBytes(ByteBuffer.wrap(iid))
        this["_system_from"].writeLong(0)
        this["_valid_from"].writeLong(0)
        this["_valid_to"].writeLong(Long.MAX_VALUE)
        this["op"].vectorFor("put", STRUCT_TYPE, false).writeObject(doc)
        endRow()
    }

    private fun Relation.delete(iid: ByteArray) {
        this["_iid"].writeBytes(ByteBuffer.wrap(iid))
        this["_system_from"].writeLong(0)
        this["_valid_from"].writeLong(0)
        this["_valid_to"].writeLong(Long.MAX_VALUE)
        this["op"]["delete"].writeNull()
        endRow()
    }

    private fun hashOf(value: Any) = Vector.fromList(al, "v", listOf(value)).use { IndexHash.of(it, 0) }

    private data class Row(val hash: Long, val iid: List<Byte>)

    private fun row(hash: Long, iid: ByteArray) = Row(hash, iid.toList())

    private fun readPage(path: Path, pageIdx: Int): List<Row> = runBlocking {
        Relation(al, bp.getFooter(path).schema).use { rel ->
            bp.getRecordBatch(path, pageIdx).use { rel.load(it) }
            val hashes = rel["_hash"]
            val iids = rel["_iid"]
            List(rel.rowCount) { idx ->
                Row(hashes.getLong(idx), iids.getBytes(idx).let { buf -> ByteArray(buf.remaining()).also { buf.duplicate().get(it) } }.toList())
            }
        }
    }

    private fun writeL0(blockIdx: Long, pageLimit: Int = 1024, writeEvents: Relation.() -> Unit): IndexTrie? =
        Trie.openLogDataWriter(al).use { rel ->
            rel.writeEvents()
            runBlocking { IndexTrieWriter(al, bp, pageLimit).writeL0(files, blockIdx, rel, "email") }
        }

    @Test
    fun `values that = holds equal hash equal, across union legs and signed zeros`() {
        assertEquals(hashOf(1L), hashOf(1.0))
        assertEquals(hashOf(1L), hashOf(1))
        assertEquals(hashOf(0.0), hashOf(-0.0))
        assertNotEquals(hashOf("ada@example.com"), hashOf("bob@example.com"))
    }

    @Test
    fun `an L0 holds one entry per distinct put value and entity, and none for a NULL, an absent column or a delete`() {
        val trie = writeL0(3) {
            put(iid(1), mapOf("email" to "ada@example.com"))
            put(iid(1), mapOf("email" to "ada@example.com"))
            put(iid(2), mapOf("email" to null))
            put(iid(3), mapOf("name" to "Carol"))
            delete(iid(4))
            put(iid(5), mapOf("email" to "eve@example.com"))
            put(iid(6), mapOf("email" to 1L))
            put(iid(7), mapOf("email" to 1.0))
        }

        assertEquals(Trie.l0Key(3), trie?.key)

        val expected = listOf(
            row(hashOf("ada@example.com"), iid(1)),
            row(hashOf("eve@example.com"), iid(5)),
            row(hashOf(1L), iid(6)),
            row(hashOf(1L), iid(7)),
        ).sortedWith { a, b -> java.lang.Long.compareUnsigned(a.hash, b.hash).takeIf { it != 0 } ?: a.iid[15].compareTo(b.iid[15]) }

        val dataPath = files.dataFilePath(trie!!.key.toString())
        assertEquals(Path.of("tables/public\$docs/indices/00000000-0000-0000-0000-00000000000a/data/l00-rc-b03.arrow"), dataPath)
        assertEquals(expected, readPage(dataPath, 0))
        assertEquals(bp.listAllObjects(dataPath.parent).single().size, trie.dataFileSize)
    }

    @Test
    fun `a block where no put row gives the column a value writes no L0`() {
        assertNull(writeL0(0) {
            put(iid(1), mapOf("name" to "Ada"))
            put(iid(2), mapOf("email" to null))
            delete(iid(3))
        })

        assertEquals(emptyList<Any>(), bp.listAllObjects().toList())
    }

    @Test
    fun `entries beyond a page branch on the hash's top bits, every leaf holding the entries under its path`() {
        val trie = writeL0(0, pageLimit = 4) {
            for (n in 0 until 200) put(iid(n), mapOf("email" to "user-$n@example.com"))
        }!!

        val trieKey = trie.key.toString()
        val metaPath = files.metaFilePath(trieKey)
        val dataPath = files.dataFilePath(trieKey)

        val leaves = mutableListOf<ArrowHashTrie.Leaf>()
        runBlocking {
            Relation(al, bp.getFooter(metaPath).schema).use { metaRel ->
                bp.getRecordBatch(metaPath, 0).use { metaRel.load(it) }
                ArrowHashTrie(metaRel["nodes"]).use { hashTrie ->
                    fun walk(node: HashTrie.Node<ArrowHashTrie.Leaf>?) {
                        when (node) {
                            null -> {}
                            is ArrowHashTrie.Leaf -> leaves += node
                            is HashTrie.IidBranch -> node.hashChildren.forEach(::walk)
                            else -> error("unexpected node: $node")
                        }
                    }
                    walk(hashTrie.rootNode)
                }
            }
        }

        assertTrue(leaves.size > 1)

        val rows = leaves.flatMap { leaf ->
            readPage(dataPath, leaf.dataPageIndex).onEach { row ->
                assertEquals(leaf.path.toList(), List(leaf.path.size) { depth -> indexBucket(row.hash, depth).toByte() })
            }.also { assertTrue(it.size <= 4) }
        }

        assertEquals((0 until 200).map { row(hashOf("user-$it@example.com"), iid(it)) }.toSet(), rows.toSet())
        assertEquals(200, rows.size)
        assertEquals(rows.sortedWith { a, b -> java.lang.Long.compareUnsigned(a.hash, b.hash) }, rows)
    }
}
