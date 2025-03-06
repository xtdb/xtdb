package xtdb.compactor

import com.carrotsearch.hppc.ByteArrayList
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.Relation
import xtdb.trie.ISegment
import xtdb.trie.Trie.dataRelSchema
import xtdb.trie.toMergePlan
import xtdb.types.Fields.mergeFields
import xtdb.types.withName
import xtdb.util.closeOnCatch
import xtdb.util.openWritableChannel
import xtdb.util.useTempFile
import java.nio.channels.WritableByteChannel
import java.util.*
import java.util.function.Predicate
import kotlin.math.min

class SegmentMerge(private val al: BufferAllocator) {
    companion object {
        private fun logDataRelSchema(dataSchemas: Collection<Schema>) =
            mergeFields(dataSchemas.map { it.findField("op").children.first() })
                .withName("put")
                .let { dataRelSchema(it) }

        private fun ByteArray.toPathPredicate() =
            Predicate<ByteArray> { pagePath ->
                val len = min(size, pagePath.size)
                Arrays.equals(this, 0, len, pagePath, 0, len)
            }
    }

    internal fun List<ISegment<*>>.mergeTo(ch: WritableByteChannel, pathFilter: ByteArray?): List<PageTree.Leaf> {
        val schema = logDataRelSchema(this.map { it.dataRel!!.schema })
        val mergePlan = this.toMergePlan(pathFilter?.toPathPredicate())

        return Relation(al, schema).use { dataRel ->
            dataRel.startUnload(ch).use { unloader ->
                with(PageMerge(dataRel, pathFilter)) {
                    var idx = 0
                    mergePlan.mapNotNull { task ->
                        if (Thread.interrupted()) throw InterruptedException()

                        mergePages(task)

                        if (dataRel.rowCount == 0) return@mapNotNull null

                        unloader.writePage()
                        PageTree.Leaf(idx++, ByteArrayList.from(*task.path), dataRel.rowCount)
                            .also { dataRel.clear() }
                    }
                }.also { unloader.end() }
            }
        }
    }

    fun List<ISegment<*>>.mergeToRelation(part: ByteArray?) =
        useTempFile("merged-segments", ".arrow") { tempFile ->
            mergeTo(tempFile.openWritableChannel(), part)

            Relation.loader(al, tempFile).use { inLoader ->
                val schema = inLoader.schema
                Relation(al, schema).closeOnCatch { outRel ->
                    Relation(al, schema).use { inRel ->
                        while (inLoader.loadNextPage(inRel))
                            outRel.append(inRel)
                    }

                    outRel
                }
            }
        }
}