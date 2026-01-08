package xtdb.operator.join

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.Relation
import xtdb.arrow.Relation.RelationUnloader
import xtdb.arrow.Vector.Companion.openVector
import xtdb.arrow.VectorWriter
import xtdb.expression.map.IndexHasher.Companion.hasher
import xtdb.trie.ColumnName
import xtdb.arrow.VectorType.Companion.I32
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.util.closeOnCatch
import xtdb.util.deleteOnCatch
import java.nio.file.Files.createTempFile
import java.nio.file.Path
import kotlin.io.path.deleteIfExists

internal const val HASH_COL_NAME = "xt/join-hash"

class Shuffle private constructor(
    private val al: BufferAllocator, private val inDataRel: Relation, hashColNames: List<ColumnName>,

    val dataFile: Path, private val outDataRel: Relation, private val dataUnloader: RelationUnloader,
    val hashFile: Path, private val hashRel: Relation, private val hashUnloader: RelationUnloader,

    expectedRowCount: Long, expectedBlockCount: Int, minParts: Int
) : AutoCloseable {

    val schema get() = inDataRel.schema

    val partCount: Int = (expectedBlockCount.takeHighestOneBit() shl 1).coerceAtLeast(minParts)
    private val hashMask: Int = partCount - 1
    private val approxRowsPerPart =
        (expectedRowCount / expectedBlockCount.coerceAtLeast(1) / partCount.coerceAtLeast(1)).toInt()
    private val hashCol = hashRel[HASH_COL_NAME]

    private val dataRowCopier = inDataRel.rowCopier(outDataRel)
    private val hasher = inDataRel.hasher(hashColNames)

    fun shuffle() {
        val selections = Array(partCount) { IntArrayList(approxRowsPerPart) }

        al.openVector(HASH_COL_NAME, I32).use { tmpHashCol ->
            repeat(inDataRel.rowCount) { inIdx ->
                val hashCode = hasher.hashCode(inIdx)
                tmpHashCol.writeInt(hashCode)
                selections[hashCode and hashMask].add(inIdx)
            }

            val hashCopier = tmpHashCol.rowCopier(hashCol)

            for (selection in selections) {
                outDataRel.clear()
                hashRel.clear()

                val selArray = selection.toArray()
                dataRowCopier.copyRows(selArray)
                hashCopier.copyRows(selArray)

                dataUnloader.writePage()
                hashUnloader.writePage()
            }
        }
    }

    fun end() {
        dataUnloader.end()
        hashUnloader.end()
    }

    private var reloader: Relation.Loader? = null
    private var reloadDataRel: Relation? = null

    fun loadDataPart(dataRel: Relation, partIdx: Int) {
        dataRel.clear()
        val reloader = this.reloader ?: Relation.loader(al, dataFile).also { this.reloader = it }
        val reloadDataRel = this.reloadDataRel ?: Relation(al, reloader.schema).also { this.reloadDataRel = it }

        repeat(reloader.pageCount / partCount) { i ->
            reloader.loadPage(i * partCount + partIdx, reloadDataRel)
            dataRel.append(reloadDataRel)
        }
    }

    private var hashReloader: Relation.Loader? = null
    private var reloadHashRel: Relation? = null

    fun loadHashPart(hashCol: VectorWriter, partIdx: Int) {
        hashCol.clear()
        val reloader = this.hashReloader ?: Relation.loader(al, hashFile).also { this.hashReloader = it }
        val reloadHashRel = this.reloadHashRel ?: Relation(al, reloader.schema).also { this.reloadHashRel = it }

        val inCol = reloadHashRel[HASH_COL_NAME]
        repeat(reloader.pageCount / partCount) { i ->
            reloader.loadPage(i * partCount + partIdx, reloadHashRel)
            hashCol.append(inCol)
        }
    }

    override fun close() {
        reloadHashRel?.close()
        hashReloader?.close()
        reloadDataRel?.close()
        reloader?.close()

        dataUnloader.close()
        outDataRel.close()

        hashUnloader.close()
        hashRel.close()

        hashFile.deleteIfExists()
        dataFile.deleteIfExists()
    }

    companion object {
        // my kingdom for `util/with-close-on-catch` in Kotlin
        // y'all need macros. or monads.

        fun open(
            al: BufferAllocator, inDataRel: Relation, hashColNames: List<ColumnName>,
            rowCount: Long, blockCount: Int, minParts: Int = 1
        ): Shuffle =
            createTempFile("xtdb-build-side-shuffle-", ".arrow").deleteOnCatch { dataFile ->
                Relation(al, inDataRel.schema).closeOnCatch { outDataRel ->
                    outDataRel.startUnload(dataFile).closeOnCatch { dataUnloader ->

                        createTempFile("xtdb-build-side-shuffle-hash-", ".arrow").deleteOnCatch { hashFile ->
                            Relation(al, HASH_COL_NAME ofType I32).closeOnCatch { outHashRel ->
                                outHashRel.startUnload(hashFile).closeOnCatch { hashUnloader ->

                                    Shuffle(
                                        al, inDataRel, hashColNames,
                                        dataFile, outDataRel, dataUnloader,
                                        hashFile, outHashRel, hashUnloader,
                                        rowCount, blockCount, minParts
                                    )

                                }
                            }
                        }

                    }
                }
            }
    }
}
