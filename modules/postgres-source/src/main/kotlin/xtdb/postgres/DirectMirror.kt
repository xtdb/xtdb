package xtdb.postgres

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.error.Incorrect
import xtdb.indexer.OpenTx
import xtdb.postgres.proto.DirectMirrorConfig
import xtdb.postgres.proto.directMirrorConfig
import xtdb.time.InstantUtil.asMicros
import xtdb.util.asIid
import java.nio.ByteBuffer
import java.time.ZonedDateTime
import com.google.protobuf.Any as ProtoAny

private const val PROTO_TAG_PREFIX = "proto.xtdb.com"

/**
 * The default [PgIndexer]: mirrors the upstream into XT as-is. A row from `public.users` lands in
 * the `users` table; `_id` is taken from the row, with explicit `_valid_from`/`_valid_to` columns
 * honoured when present.
 */
class DirectMirror : PgIndexer {
    private fun explicitValidTimeMicros(name: String, value: Any?): Long? = when (value) {
        null -> null
        is ZonedDateTime -> value.toInstant().asMicros
        else -> throw Incorrect(
            "'$name' must be a TIMESTAMPTZ column",
            errorCode = "xtdb.postgres/invalid-explicit-valid-time",
            data = mapOf("column" to name, "type" to (value::class.qualifiedName ?: value::class.simpleName)),
        )
    }

    private fun writeOp(openTx: OpenTx, op: RowOp) {
        val openTxTable = openTx.table(op.schema, op.table)

        when (op) {
            is RowOp.Put -> {
                val docMap = op.row.toMutableMap()

                val id = docMap["_id"]
                    ?: throw Incorrect("Missing '_id' in row from ${op.schema}.${op.table}")

                val explicitValidFrom = explicitValidTimeMicros("_valid_from", docMap.remove("_valid_from"))
                val explicitValidTo = explicitValidTimeMicros("_valid_to", docMap.remove("_valid_to"))

                if (explicitValidTo != null && explicitValidFrom == null)
                    throw Incorrect("'_valid_to' requires '_valid_from'")

                openTxTable.apply {
                    writeId(id)
                    writeValidTimeMicros(
                        explicitValidFrom ?: openTx.systemTimeMicros,
                        explicitValidTo ?: Long.MAX_VALUE,
                    )
                    putDocWriter.writeObject(docMap)
                    endPut()
                }
            }

            is RowOp.Delete -> {
                val id = op.row["_id"]
                    ?: throw Incorrect("Missing '_id' in delete on ${op.schema}.${op.table}")

                openTxTable.apply {
                    writeId(id)
                    writeDefaultValidTime()
                    endDelete()
                }
            }
        }
    }

    override fun indexTx(tx: PostgresDriver.Transaction, openTx: OpenTx) {
        for (op in tx.ops) writeOp(openTx, op)
    }

    @Serializable
    @SerialName("!DirectMirror")
    class Factory : PgIndexer.Factory {

        override fun open(): PgIndexer = DirectMirror()

        override fun equals(other: Any?) = other is Factory
        override fun hashCode() = javaClass.hashCode()

        class Registration : PgIndexer.Registration<Factory> {
            override val protoTag: String
                get() = "$PROTO_TAG_PREFIX/xtdb.postgres.proto.DirectMirrorConfig"

            override val factoryClass get() = Factory::class.java

            override fun toProto(factory: Factory): ProtoAny =
                ProtoAny.pack(directMirrorConfig { }, PROTO_TAG_PREFIX)

            override fun fromProto(msg: ProtoAny): PgIndexer.Factory {
                msg.unpack(DirectMirrorConfig::class.java)
                return Factory()
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<PgIndexer.Factory>) {
                builder.subclass(Factory::class)
            }
        }
    }
}
