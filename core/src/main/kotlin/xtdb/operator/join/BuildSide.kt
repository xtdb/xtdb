package xtdb.operator.join

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.ArrowFileReader
import org.apache.arrow.vector.types.Types
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.roaringbitmap.RoaringBitmap
import xtdb.arrow.*
import xtdb.expression.map.IndexHasher
import xtdb.util.openReadableChannel
import xtdb.util.openWritableChannel
import xtdb.vector.OldRelationWriter
import java.nio.file.Files
import java.nio.file.Path
import java.util.function.IntConsumer
import java.util.function.IntUnaryOperator
import kotlin.io.path.createDirectories
import kotlin.io.path.deleteIfExists

internal const val NULL_ROW_IDX = 0

class BuildSide(
    val al: BufferAllocator,
    val schema: Schema,
    val keyColNames: List<String>,
    val matchedBuildIdxs: RoaringBitmap?,
    private val withNilRow: Boolean,
    private val inMemoryThreshold: Int = 100_000
) : AutoCloseable {

    internal var toDisk: Boolean = false
    private val diskSchema: Schema =  Schema(schema.fields.plus(HASH_COLUMN_FIELD))
    private val tmpFile: Path =
        Files.createTempDirectory("xtdb-build-side-tmp").let { rootPath ->
            Files.createTempFile(rootPath.createDirectories(), "build-side", ".arrow") }

    private fun internalRelToExternalRel(internalRel: OldRelationWriter) =
        OldRelationWriter(al, internalRel.vectors.filter { it.name != HASH_COLUMN_NAME }).also { it.rowCount = internalRel.rowCount }

    private fun internalRelToHashColumn(internalRel: OldRelationWriter) = internalRel[HASH_COLUMN_NAME]

    // The reference counting should happen on the internalRel
    private val internalRel = OldRelationWriter(al, diskSchema)
    private val externalRel = internalRelToExternalRel(internalRel)
    private val hashColumn: VectorWriter = internalRelToHashColumn(internalRel)
    private val unloadChannel = tmpFile.openWritableChannel()
    private val unloader = ArrowUnloader.open(unloadChannel, diskSchema, ArrowUnloader.Mode.FILE)

    var buildMap: BuildSideMap? = null

    init {
        if (withNilRow) {
            hashColumn.writeInt(0)
            externalRel.endRow()
            internalRel.rowCount = 1
        }
    }

    companion object {
        const val HASH_COLUMN_NAME = "xt/join-hash"
        val HASH_COLUMN_FIELD: Field = Field.notNullable(HASH_COLUMN_NAME, Types.MinorType.INT.type)
    }

    private fun finishBatch() {
        internalRel.rowCount = externalRel.rowCount
        if (toDisk) {
            internalRel.asReader.openDirectSlice(al).use { rel ->
                rel.openArrowRecordBatch().use { recordBatch ->
                    unloader.writeBatch(recordBatch)
                }
            }
            hashColumn.clear()
            externalRel.clear()
            internalRel.clear()
        }
    }

    @Suppress("NAME_SHADOWING")
    fun append(inRel: RelationReader) {
        val inKeyCols = keyColNames.map { inRel.vectorForOrNull(it) as VectorReader }

        val hasher = IndexHasher.fromCols(inKeyCols)
        val rowCopier = externalRel.rowCopier(inRel)

        repeat(inRel.rowCount) { inIdx ->
            hashColumn.writeInt(hasher.hashCode(inIdx))
            rowCopier.copyRow(inIdx)
        }

        if (externalRel.rowCount > inMemoryThreshold) {
            toDisk = true
            finishBatch()
        }
    }

    private fun readInRels() {
        if (toDisk) {
            internalRel.clear()
            ArrowFileReader(tmpFile.openReadableChannel(), al).use { rdr ->
                while (rdr.loadNextBatch()) {
                    val root = rdr.vectorSchemaRoot
                    RelationReader.from(root).use { inRel ->
                        internalRel.append(inRel)
                    }
                }
            }
        }
    }

    fun build() {
        buildMap?.close()
        finishBatch()
        unloader.end()
        readInRels()
        buildMap = BuildSideMap.from(
            al,
            internalRelToHashColumn(internalRel).asReader.let { if (withNilRow) it.select(1, it.valueCount.dec()) else it },
            if (withNilRow) 1 else 0)
    }

    val builtRel get() = internalRelToExternalRel(internalRel).asReader

    fun addMatch(idx: Int) = matchedBuildIdxs?.add(idx)

    fun indexOf(hashCode: Int, cmp: IntUnaryOperator, removeOnMatch: Boolean): Int =
        requireNotNull(buildMap).findValue(hashCode, cmp, removeOnMatch)

    fun forEachMatch(hashCode: Int, c: IntConsumer) =
        requireNotNull(buildMap).forEachMatch(hashCode, c)

    override fun close() {
        buildMap?.close()
        internalRel.close()
        unloader.close()
        unloadChannel.close()
        tmpFile.deleteIfExists()
    }
}
