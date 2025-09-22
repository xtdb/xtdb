package xtdb.operator.join

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.Relation
import xtdb.util.closeOnCatch
import xtdb.util.deleteOnCatch
import java.nio.file.Files
import java.nio.file.Path

internal class Spill(
    private val al: BufferAllocator, private val inRel: Relation,
    val path: Path, private val unloader: Relation.RelationUnloader,
) : AutoCloseable {

    var blockCount: Int = 0; private set
    var rowCount: Long = 0; private set

    fun spill() {
        if (inRel.rowCount == 0) return
        rowCount += inRel.rowCount
        blockCount++
        unloader.writePage()
        inRel.clear()
    }

    fun openDataLoader() = Relation.loader(al, path)

    fun loadAll(outRel: Relation) {
        outRel.clear()
        Relation(al, inRel.schema).use { tmpRel ->
            openDataLoader().use { loader ->
                while (loader.loadNextPage(tmpRel)) {
                    outRel.append(tmpRel)
                }
            }
        }
    }

    fun end() {
        unloader.end()
    }

    override fun close() {
        unloader.close()
        Files.deleteIfExists(path)
    }

    companion object {
        fun open(al: BufferAllocator, dataRel: Relation): Spill =
            Files.createTempFile("xtdb-build-side-", ".arrow").deleteOnCatch { dataPath ->
                dataRel.startUnload(dataPath).closeOnCatch { dataUnloader ->
                    Spill(al, dataRel, dataPath, dataUnloader)
                }
            }
    }
}