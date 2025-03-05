package xtdb.compactor

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.Relation
import xtdb.trie.DataRel
import xtdb.trie.ISegment
import xtdb.trie.Trie.dataRelSchema
import xtdb.trie.toMergePlan
import xtdb.types.Fields.mergeFields
import xtdb.types.withName
import xtdb.util.closeOnCatch
import xtdb.util.openWritableChannel
import xtdb.util.useTempFile
import java.nio.channels.WritableByteChannel
import java.nio.file.Path
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

    private fun List<ISegment<*>>.openOutRelation() =
        Relation(al, logDataRelSchema(this.map { it.dataRel!!.schema }))

    private fun List<ISegment<*>>.mergeTo(ch: WritableByteChannel, pathFilter: ByteArray?) {
        openOutRelation().use { dataRel ->
            dataRel.startUnload(ch).use { unloader ->
                with(PageMerge(dataRel, pathFilter)) {
                    toMergePlan(pathFilter?.toPathPredicate())
                        .forEach { task ->
                            if (Thread.interrupted()) throw InterruptedException()

                            mergePages(task)

                            unloader.writePage()
                            dataRel.clear()
                        }
                }

                unloader.end()
            }
        }
    }

    private fun Path.readSegments() =
        Relation.loader(al, this).use { inLoader ->
            val schema = inLoader.schema
            Relation(al, schema).closeOnCatch { outRel ->
                Relation(al, schema).use { inRel ->
                    while (inLoader.loadNextPage(inRel))
                        outRel.append(inRel)
                }

                outRel
            }
        }

    fun List<ISegment.Segment<*>>.mergeToRelation(part: ByteArray?) =
        useTempFile("merged-segments", ".arrow") { tempFile ->
            mergeTo(tempFile.openWritableChannel(), part)
            tempFile.readSegments()
        }
}