package xtdb.postgres

import com.google.protobuf.Struct
import com.google.protobuf.Value
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.modules.PolymorphicModuleBuilder
import kotlinx.serialization.modules.subclass
import xtdb.indexer.OpenTx
import com.google.protobuf.Any as ProtoAny

/**
 * A genuinely-registered, test-only [PgIndexer] proving the SPI seam end-to-end: a third-party
 * indexer that re-routes every op to a fixed `target` table and drops `dropColumn` on the way
 * through. Registered via `META-INF/services` in the test source set and selected through
 * `indexer: !TestReroute` in ATTACH YAML.
 *
 * Its proto carrier is a [Struct] (a well-known type) rather than a generated message — no module
 * generates protobuf from a test source set, and a unique `typeUrl` plus the two string params is
 * all the registry round-trip needs.
 */
class TestRerouteIndexer(
    private val target: String,
    private val dropColumn: String,
) : PgIndexer {

    override fun indexTx(tx: PostgresDriver.Transaction, openTx: OpenTx) {
        val rerouted = tx.ops.map { op ->
            when (op) {
                is RowOp.Put -> RowOp.Put("public", target, op.row - dropColumn)
                is RowOp.Delete -> RowOp.Delete("public", target, op.row)
            }
        }
        DirectMirror().indexTx(PostgresDriver.Transaction(tx.lsn, tx.commitTime, rerouted), openTx)
    }

    @Serializable
    @SerialName("!TestReroute")
    data class Factory(val target: String, val dropColumn: String) : PgIndexer.Factory {

        override fun open(): PgIndexer = TestRerouteIndexer(target, dropColumn)

        class Registration : PgIndexer.Registration<Factory> {
            override val protoTag: String get() = "$PROTO_TAG_PREFIX/google.protobuf.Struct"

            override val factoryClass get() = Factory::class.java

            override fun toProto(factory: Factory): ProtoAny = ProtoAny.pack(
                Struct.newBuilder()
                    .putFields("target", Value.newBuilder().setStringValue(factory.target).build())
                    .putFields("dropColumn", Value.newBuilder().setStringValue(factory.dropColumn).build())
                    .build(),
                PROTO_TAG_PREFIX,
            )

            override fun fromProto(msg: ProtoAny): PgIndexer.Factory {
                val struct = msg.unpack(Struct::class.java)
                return Factory(
                    target = struct.getFieldsOrThrow("target").stringValue,
                    dropColumn = struct.getFieldsOrThrow("dropColumn").stringValue,
                )
            }

            override fun registerSerde(builder: PolymorphicModuleBuilder<PgIndexer.Factory>) {
                builder.subclass(Factory::class)
            }
        }

        companion object {
            private const val PROTO_TAG_PREFIX = "proto.xtdb.com"
        }
    }
}
