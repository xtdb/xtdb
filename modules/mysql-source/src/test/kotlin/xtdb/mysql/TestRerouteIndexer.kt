package xtdb.mysql

import com.google.protobuf.Struct
import com.google.protobuf.Value
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.api.tx.OpenTx
import com.google.protobuf.Any as ProtoAny

class TestRerouteIndexer(private val target: String, private val dropColumn: String) : MySqlIndexer {

    private fun RowOp.rerouted(): RowOp {
        fun withId(row: Map<String, Any?>) =
            (if ("_id" in row) row else row + ("_id" to primaryKey.map { row[it] }.singleOrNull())) - dropColumn

        return when (this) {
            is RowOp.Put -> RowOp.Put("rerouted", target, primaryKey, withId(row))
            is RowOp.Update -> RowOp.Update("rerouted", target, primaryKey, withId(before), withId(row))
            is RowOp.Delete -> RowOp.Delete("rerouted", target, primaryKey, withId(row))
        }
    }

    override fun indexTx(tx: MySqlDriver.Transaction, openTx: OpenTx) {
        val ops = tx.ops.map { it.rerouted() }

        DirectMirror().indexTx(
            when (tx) {
                is MySqlDriver.Transaction.Snapshot -> tx.copy(ops = ops.filterIsInstance<RowOp.Put>())
                is MySqlDriver.Transaction.Committed -> tx.copy(ops = ops)
            },
            openTx,
        )
    }

    @Serializable
    @SerialName("!TestReroute")
    data class Factory(val target: String, val dropColumn: String) : MySqlIndexer.Factory {

        override fun open(): MySqlIndexer = TestRerouteIndexer(target, dropColumn)

        class Registration : MySqlIndexer.Registration<Factory> {
            override val protoTag: String get() = "proto.xtdb.com/google.protobuf.Struct"

            override val factoryClass get() = Factory::class.java

            override fun toProto(factory: Factory): ProtoAny = ProtoAny.pack(
                Struct.newBuilder()
                    .putFields("target", Value.newBuilder().setStringValue(factory.target).build())
                    .putFields("dropColumn", Value.newBuilder().setStringValue(factory.dropColumn).build())
                    .build(),
                "proto.xtdb.com",
            )

            override fun fromProto(msg: ProtoAny): MySqlIndexer.Factory {
                val struct = msg.unpack(Struct::class.java)
                return Factory(struct.getFieldsOrThrow("target").stringValue, struct.getFieldsOrThrow("dropColumn").stringValue)
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<MySqlIndexer.Factory>) {
                builder.subclass(Factory::class)
            }
        }
    }
}
