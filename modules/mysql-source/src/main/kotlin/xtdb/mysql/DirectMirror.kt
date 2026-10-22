package xtdb.mysql

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.api.error.Incorrect
import xtdb.api.tx.OpenTx
import xtdb.mysql.proto.DirectMirrorConfig
import xtdb.mysql.proto.directMirrorConfig
import xtdb.time.InstantUtil.asMicros
import java.time.ZonedDateTime
import java.util.Objects
import com.google.protobuf.Any as ProtoAny

private const val PROTO_TAG_PREFIX = "proto.xtdb.com"

/**
 * Mirrors the upstream into XT as-is: a row of `shop.users` lands in the `users` table of the XT schema `shop`.
 * `_id` is taken from the row, and explicit `_valid_from`/`_valid_to` columns are honoured — as `TIMESTAMP`
 * columns, which are instants, where a `DATETIME` is a wall-clock reading in no zone.
 */
class DirectMirror : MySqlIndexer {
    private fun explicitValidTimeMicros(name: String, value: Any?): Long? = when (value) {
        null -> null
        is ZonedDateTime -> value.toInstant().asMicros
        else -> throw Incorrect(
            "'$name' must be a TIMESTAMP column",
            errorCode = "xtdb.mysql/invalid-explicit-valid-time",
            data = mapOf("column" to name, "type" to (value::class.qualifiedName ?: value::class.simpleName)),
        )
    }

    private fun OpenTx.put(op: RowOp, row: Map<String, Any?>) {
        val docMap = row.toMutableMap()

        val id = docMap["_id"]
            ?: throw Incorrect("Missing '_id' in row from ${op.database}.${op.table}")

        val explicitValidFrom = explicitValidTimeMicros("_valid_from", docMap.remove("_valid_from"))
        val explicitValidTo = explicitValidTimeMicros("_valid_to", docMap.remove("_valid_to"))

        if (explicitValidTo != null && explicitValidFrom == null)
            throw Incorrect("'_valid_to' requires '_valid_from'")

        table(op.database, op.table).apply {
            writeId(id)
            writeValidTimeMicros(explicitValidFrom ?: systemTimeMicros, explicitValidTo ?: Long.MAX_VALUE)
            putDocWriter.writeObject(docMap)
            endPut()
        }
    }

    private fun OpenTx.delete(op: RowOp, row: Map<String, Any?>) {
        val id = row["_id"]
            ?: throw Incorrect("Missing '_id' in delete on ${op.database}.${op.table}")

        table(op.database, op.table).apply {
            writeId(id)
            writeDefaultValidTime()
            endDelete()
        }
    }

    override fun indexTx(tx: MySqlDriver.Transaction, openTx: OpenTx) {
        for (op in tx.ops) {
            when (op) {
                is RowOp.Put -> openTx.put(op, op.row)

                is RowOp.Update -> {
                    if (!Objects.deepEquals(op.before["_id"], op.row["_id"])) openTx.delete(op, op.before)
                    openTx.put(op, op.row)
                }

                is RowOp.Delete -> openTx.delete(op, op.row)
            }
        }
    }

    @Serializable
    @SerialName("!DirectMirror")
    class Factory : MySqlIndexer.Factory {

        override fun open(): MySqlIndexer = DirectMirror()

        override fun equals(other: Any?) = other is Factory
        override fun hashCode() = javaClass.hashCode()

        class Registration : MySqlIndexer.Registration<Factory> {
            override val protoTag: String
                get() = "$PROTO_TAG_PREFIX/xtdb.mysql.proto.DirectMirrorConfig"

            override val factoryClass get() = Factory::class.java

            override fun toProto(factory: Factory): ProtoAny =
                ProtoAny.pack(directMirrorConfig { }, PROTO_TAG_PREFIX)

            override fun fromProto(msg: ProtoAny): MySqlIndexer.Factory {
                msg.unpack(DirectMirrorConfig::class.java)
                return Factory()
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<MySqlIndexer.Factory>) {
                builder.subclass(Factory::class)
            }
        }
    }
}
