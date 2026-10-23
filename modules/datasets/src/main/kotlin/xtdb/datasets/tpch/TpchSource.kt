package xtdb.datasets.tpch

import io.airlift.tpch.TpchColumn
import io.airlift.tpch.TpchColumnType.Base
import io.airlift.tpch.TpchEntity
import io.airlift.tpch.TpchTable
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.error.Incorrect
import xtdb.api.tx.ExternalSource
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.tx.TxIndexer
import xtdb.api.tx.TxIndexer.TxResult
import xtdb.arrow.Relation
import xtdb.arrow.RelationAsStructReader
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.DATE_DAY
import xtdb.arrow.VectorType.Companion.F64
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.IID
import xtdb.arrow.VectorType.Companion.UTF8
import xtdb.arrow.VectorType.Companion.UUID
import xtdb.arrow.VectorWriter
import xtdb.datasets.proto.TpchSourceConfig
import xtdb.datasets.proto.tpchSourceConfig
import xtdb.util.asIid
import xtdb.util.closeOnCatch
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicReference
import com.google.protobuf.Any as ProtoAny
import java.util.UUID as JUUID

private const val PROTO_TAG_PREFIX = "proto.xtdb.com"
private const val BATCH_SIZE = 65536

// carried only by the empty tx after the last batch, so a token is indexed iff a load ran to completion
private val LOADED: ExternalSourceToken = "tpch-loaded".toByteArray()

private val PK_COLS = mapOf(
    "part" to listOf("p_partkey"),
    "supplier" to listOf("s_suppkey"),
    "partsupp" to listOf("ps_partkey", "ps_suppkey"),
    "customer" to listOf("c_custkey"),
    "lineitem" to listOf("l_orderkey", "l_linenumber"),
    "orders" to listOf("o_orderkey"),
    "nation" to listOf("n_nationkey"),
    "region" to listOf("r_regionkey"),
)

private val ByteArray.asUuid get() = ByteBuffer.wrap(this).let { JUUID(it.getLong(), it.getLong()) }

// an IDENTIFIER cell is the iid of e.g. "orderkey_42", as a UUID
private fun identifierIid(col: TpchColumn<TpchEntity>): (TpchEntity) -> ByteArray {
    val prefix = col.columnName.replace(Regex(".+_"), "") + "_"
    return { e -> (prefix + col.getIdentifier(e)).asIid }
}

private fun pkIid(pk: List<TpchColumn<TpchEntity>>): (TpchEntity) -> ByteArray {
    pk.singleOrNull()?.let { return identifierIid(it) }

    val parts = pk.map { col ->
        if (col.type.base == Base.IDENTIFIER) identifierIid(col).let { iid -> { e: TpchEntity -> iid(e).asUuid.toString() } }
        else { e: TpchEntity -> col.getInteger(e).toString() }
    }

    return { e -> parts.joinToString("___") { it(e) }.asIid }
}

private class Column(val name: String, val type: VectorType, val write: (VectorWriter, TpchEntity) -> Unit)

@Suppress("UNCHECKED_CAST")
private fun column(col: TpchColumn<*>): Column {
    col as TpchColumn<TpchEntity>
    val name = col.columnName

    return when (col.type.base!!) {
        Base.IDENTIFIER -> {
            val iid = identifierIid(col)
            Column(name, UUID) { out, e -> out.writeBytes(iid(e)) }
        }

        Base.INTEGER -> Column(name, I64) { out, e -> out.writeLong(col.getInteger(e).toLong()) }
        Base.VARCHAR -> Column(name, UTF8) { out, e -> out.writeObject(col.getString(e)) }
        Base.DOUBLE -> Column(name, F64) { out, e -> out.writeDouble(col.getDouble(e)) }
        Base.DATE -> Column(name, DATE_DAY) { out, e -> out.writeInt(col.getDate(e)) }
    }
}


private class TableBatches(private val table: TpchTable<*>) {
    private val cols = table.columns.map(::column)

    @Suppress("UNCHECKED_CAST")
    private val iid = pkIid(PK_COLS.getValue(table.tableName).map { table.getColumn(it) as TpchColumn<TpchEntity> })

    private val types =
        linkedMapOf<String, VectorType>("_iid" to IID, "_id" to UUID).apply { for (col in cols) put(col.name, col.type) }

    fun openBatch(al: BufferAllocator, entities: Iterator<TpchEntity>): Relation =
        Relation(al, types).closeOnCatch { rel ->
            val iidVec = rel["_iid"]
            val idVec = rel["_id"]
            val colVecs = cols.map { rel[it.name] }

            var rowCount = 0
            while (rowCount < BATCH_SIZE && entities.hasNext()) {
                val e = entities.next()
                val iid = iid(e)
                iidVec.writeBytes(iid)
                idVec.writeBytes(iid)
                for (i in cols.indices) cols[i].write(colVecs[i], e)
                rowCount++
            }

            rel.rowCount = rowCount
            rel
        }

    fun putsReader(batch: Relation): RelationReader =
        RelationReader.from(
            listOf(
                batch["_iid"],
                RelationAsStructReader("doc", RelationReader.from(batch.vectors.filter { it.name != "_iid" }, batch.rowCount)),
            ),
            batch.rowCount,
        )

    val tableName: String get() = table.tableName

    @Suppress("UNCHECKED_CAST")
    fun generator(scaleFactor: Double, part: Int, partCount: Int): Iterator<TpchEntity> =
        table.createGenerator(scaleFactor, part, partCount).iterator() as Iterator<TpchEntity>
}

/**
 * An external source that writes the TPC-H dataset into its database on the first leader term, then stops.
 *
 * Rows match `xtdb.datasets.tpch/submit-rels!` at the same scale factor.
 * A later term, once the load has completed, writes nothing; a load interrupted part-way starts again from scratch.
 */
class TpchSource(private val scaleFactor: Double, private val threads: Int) : ExternalSource {

    /**
     * @param scaleFactor TPC-H scale factor, finite and > 0 — 1.0 is ~6M lineitems.
     * @param threads how many slices each table is generated in, concurrently; >= 1.
     */
    @Serializable
    @SerialName("!Tpch")
    data class Factory(val scaleFactor: Double, val threads: Int = 1) : ExternalSource.Factory {

        override fun open(dbName: String, remotes: Map<RemoteAlias, Remote>, meterRegistry: MeterRegistry?): TpchSource {
            if (!scaleFactor.isFinite() || scaleFactor <= 0.0)
                throw Incorrect(
                    "TPC-H scale factor must be positive and finite", errorCode = "xtdb.tpch/invalid-scale-factor",
                    data = mapOf("scale-factor" to scaleFactor),
                )

            if (threads < 1)
                throw Incorrect(
                    "TPC-H threads must be at least 1", errorCode = "xtdb.tpch/invalid-threads",
                    data = mapOf("threads" to threads),
                )

            return TpchSource(scaleFactor, threads)
        }

        class Registration : ExternalSource.Registration<Factory> {
            override val protoTag: String get() = "$PROTO_TAG_PREFIX/xtdb.datasets.proto.TpchSourceConfig"

            override val factoryClass get() = Factory::class.java

            override fun toProto(factory: Factory): ProtoAny =
                ProtoAny.pack(tpchSourceConfig {
                    scaleFactor = factory.scaleFactor
                    threads = factory.threads
                }, PROTO_TAG_PREFIX)

            override fun fromProto(msg: ProtoAny): Factory {
                val config = msg.unpack(TpchSourceConfig::class.java)
                return Factory(config.scaleFactor, config.threads)
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<ExternalSource.Factory>) {
                builder.subclass(Factory::class)
            }
        }
    }

    private val allocator = RootAllocator()

    private suspend fun loadPart(batches: TableBatches, part: Int, txIndexer: TxIndexer) {
        val entities = batches.generator(scaleFactor, part, threads)

        while (entities.hasNext()) {
            // the writer runs on the leader term's coroutine, not this one:
            // whichever side takes the batch closes it, so a cancelled wait can't free it from under a running writer
            val batch = AtomicReference(batches.openBatch(allocator, entities))

            try {
                txIndexer.executeTx(externalSourceToken = null) { openTx ->
                    val rel = batch.getAndSet(null) ?: return@executeTx TxResult.Aborted(CancellationException("TPC-H load cancelled"))

                    rel.use { openTx.table(tableName = batches.tableName).writePuts(batches.putsReader(it)) }
                    TxResult.Committed()
                }
            } finally {
                batch.getAndSet(null)?.close()
            }
        }
    }

    override suspend fun onPartitionAssigned(partition: Int, afterToken: ExternalSourceToken?, txIndexer: TxIndexer) {
        if (afterToken != null) return

        for (table in TpchTable.getTables()) {
            val batches = TableBatches(table)

            coroutineScope {
                for (part in 1..threads) launch(Dispatchers.Default) { loadPart(batches, part, txIndexer) }
            }
        }

        txIndexer.executeTx(LOADED) { TxResult.Committed() }
    }

    override fun close() = allocator.close()
}
