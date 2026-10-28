package xtdb.compactor

import com.carrotsearch.hppc.ByteArrayList
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.RelationWriter
import xtdb.arrow.VectorType.Companion.INSTANT
import xtdb.arrow.VectorType.Companion.ofType
import xtdb.compactor.RecencyPartition.*
import xtdb.compactor.SegmentMerge.Results
import xtdb.time.microsAsInstant
import xtdb.trie.RecencyRange
import xtdb.trie.Trie
import xtdb.util.closeAll
import xtdb.util.openWritableChannel
import java.nio.file.Path
import java.time.DayOfWeek
import java.time.LocalDate
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.temporal.TemporalAdjusters
import java.time.temporal.TemporalAdjusters.next
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.createTempDirectory
import kotlin.io.path.createTempFile
import kotlin.io.path.deleteRecursively

private typealias RecencyMicros = Long

enum class RecencyPartition {
    WEEK,
    MONTH,
    QUARTER,
    YEAR;
}

internal fun ZonedDateTime.roundToNextPartition(recencyPartition: RecencyPartition): ZonedDateTime =
    // No need to round below day level as these get dropped in the TrieKey anyway
    when (recencyPartition) {
        WEEK -> this.with(next(DayOfWeek.MONDAY))
        MONTH -> this.with(TemporalAdjusters.firstDayOfNextMonth())
        QUARTER -> {
            val month = this.monthValue
            val nextQuarterMonth = when {
                month < 4 -> 4
                month < 7 -> 7
                month < 10 -> 10
                else -> 1
            }

            var result = this
            if (month >= 10) {
                result = result.plusYears(1)
            }

            result.withMonth(nextQuarterMonth)
                .withDayOfMonth(1)
        }
        YEAR -> this.with(TemporalAdjusters.firstDayOfNextYear())
    }

internal fun RecencyMicros.toPartition(recencyPartition: RecencyPartition = WEEK): LocalDate =
    microsAsInstant.minusNanos(1)
        .atZone(ZoneOffset.UTC)
        .roundToNextPartition(recencyPartition)
        .toLocalDate()

internal interface OutWriter : AutoCloseable {
    fun rowCopier(reader: RelationReader): RecencyRowCopier
    fun endPage(path: ByteArrayList)

    /**
     * assumption: so long as end() is called before the OutWriter is closed, the Results can be read afterwards
     */
    fun end(): Results

    fun interface RecencyRowCopier {
        fun copyRow(recency: RecencyMicros, sourceIndex: Int)
    }

    class OutWriters(private val al: BufferAllocator): AutoCloseable {

        private val tempDir = createTempDirectory("compactor")

        internal enum class RecencyColumn { ABSENT, CARRIED, COMPUTED }

        private fun RelationReader.withoutRecency() =
            RelationReader.from(vectors.filter { it.name != Trie.RECENCY_COL }, rowCount)

        internal inner class OutRel(
            schema: Schema,
            private val outPath: Path = createTempFile(tempDir, "merged-segments", ".arrow"),
            val recency: LocalDate?,
            private val recencyColumn: RecencyColumn
        ) : OutWriter {
            private val outRel = Relation(
                al,
                if (recencyColumn == RecencyColumn.ABSENT) schema
                else Schema(schema.fields + (Trie.RECENCY_COL ofType INSTANT))
            )

            private val unloader = runCatching { outRel.startUnload(outPath.openWritableChannel()) }
                .onFailure { outRel.close() }
                .getOrThrow()

            private var pageIdx = 0
            private var pageMinRecency = RecencyRange.EMPTY.min
            private var pageMaxRecency = RecencyRange.EMPTY.max
            private val leaves0 = mutableListOf<PageTree.Leaf>()

            val leaves: List<PageTree.Leaf> get() = leaves0

            private fun observeRecency(recency: RecencyMicros) {
                if (recency < pageMinRecency) pageMinRecency = recency
                if (recency > pageMaxRecency) pageMaxRecency = recency
            }

            override fun rowCopier(reader: RelationReader): RecencyRowCopier =
                when (recencyColumn) {
                    RecencyColumn.ABSENT -> reader.withoutRecency().rowCopier(outRel).let { copier ->
                        RecencyRowCopier { _, sourceIndex -> copier.copyRow(sourceIndex) }
                    }

                    RecencyColumn.CARRIED -> {
                        val sourceRecency = reader[Trie.RECENCY_COL]
                        val copier = reader.rowCopier(outRel)

                        RecencyRowCopier { _, sourceIndex ->
                            observeRecency(sourceRecency.getLong(sourceIndex))
                            copier.copyRow(sourceIndex)
                        }
                    }

                    RecencyColumn.COMPUTED -> {
                        val recencyVec = outRel[Trie.RECENCY_COL]

                        // the copier would otherwise fill the recency column with the source's (absent) value
                        val contentRel = object : RelationWriter by outRel {
                            override val vectors = outRel.vectors.filter { it.name != Trie.RECENCY_COL }
                            override fun vectorForOrNull(name: String) =
                                if (name == Trie.RECENCY_COL) null else outRel.vectorForOrNull(name)
                        }

                        val copier = reader.withoutRecency().rowCopier(contentRel)

                        RecencyRowCopier { recency, sourceIndex ->
                            observeRecency(recency)
                            recencyVec.writeLong(recency)
                            copier.copyRow(sourceIndex)
                        }
                    }
                }

            override fun endPage(path: ByteArrayList) {
                if (outRel.rowCount == 0) return

                unloader.writePage()

                leaves0 += PageTree.Leaf(pageIdx++, path, outRel.rowCount, RecencyRange(pageMinRecency, pageMaxRecency))
                outRel.clear()
                pageMinRecency = RecencyRange.EMPTY.min
                pageMaxRecency = RecencyRange.EMPTY.max
            }

            override fun end(): Results {
                unloader.end()
                return Results(listOf(SegmentMerge.Result(outPath, recency, leaves)))
            }

            override fun close() {
                unloader.close()
                outRel.close()
            }
        }

        internal inner class PartitionedOutWriter(
            private val schema: Schema, private val recencyPartition: RecencyPartition?,
            private val historicalRecency: RecencyColumn
        ) : OutWriter {
            private val outDir = createTempDirectory(tempDir, "merged-segments")
            private var currentRel = OutRel(schema, outDir.resolve("rc.arrow"), null, RecencyColumn.ABSENT)

            private val historicalRels = mutableMapOf<LocalDate, OutRel>()

            private fun historicalRel(recencyPartition: LocalDate) = historicalRels.computeIfAbsent(recencyPartition) {
                OutRel(schema, outDir.resolve("r${Trie.RECENCY_FMT.format(it)}.arrow"), recencyPartition, historicalRecency)
            }

            override fun rowCopier(reader: RelationReader) = object : RecencyRowCopier {
                private val currentCopier = currentRel.rowCopier(reader)
                private val historicalCopiers = mutableMapOf<LocalDate, RecencyRowCopier>()

                private fun copier(recency: RecencyMicros) =
                    if (recency == Long.MAX_VALUE) currentCopier
                    else historicalCopiers.computeIfAbsent(recency.toPartition(this@PartitionedOutWriter.recencyPartition ?: WEEK)) { historicalRel(it).rowCopier(reader) }

                override fun copyRow(recency: RecencyMicros, sourceIndex: Int) =
                    copier(recency).copyRow(recency, sourceIndex)
            }

            override fun endPage(path: ByteArrayList) {
                historicalRels.values.forEach { it.endPage(path) }
                currentRel.endPage(path)
            }

            override fun end(): Results =
                Results(historicalRels.values.flatMap { it.end() } + currentRel.end(), outDir)

            override fun close() {
                historicalRels.closeAll()
                currentRel.close()
            }
        }

        @OptIn(ExperimentalPathApi::class)
        override fun close() {
            tempDir.deleteRecursively()
        }
    }
}
