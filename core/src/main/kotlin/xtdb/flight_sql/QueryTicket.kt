package xtdb.flight_sql

import com.google.protobuf.ByteString
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import xtdb.InternalApi
import xtdb.api.error.Incorrect
import xtdb.api.query.QueryBasis
import xtdb.database.DatabaseName
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.channels.Channels
import java.util.Base64

@Serializable(QueryParams.Serde::class)
internal class QueryParams private constructor(private val bytes: ByteArray) {

    fun <R> read(allocator: BufferAllocator, f: (VectorSchemaRoot) -> R): R =
        ArrowStreamReader(ByteArrayInputStream(bytes), allocator).use { rdr ->
            rdr.loadNextBatch()
            f(rdr.vectorSchemaRoot)
        }

    internal object Serde : KSerializer<QueryParams> {
        override val descriptor = PrimitiveSerialDescriptor("xtdb.flight-sql.query-params", PrimitiveKind.STRING)

        override fun serialize(encoder: Encoder, value: QueryParams) =
            encoder.encodeString(Base64.getEncoder().encodeToString(value.bytes))

        override fun deserialize(decoder: Decoder) = QueryParams(Base64.getDecoder().decode(decoder.decodeString()))
    }

    companion object {
        fun of(root: VectorSchemaRoot) = QueryParams(
            ByteArrayOutputStream()
                .also { out ->
                    ArrowStreamWriter(root, null, Channels.newChannel(out)).use { wtr ->
                        wtr.start()
                        wtr.writeBatch()
                        wtr.end()
                    }
                }
                .toByteArray()
        )
    }
}

/**
 * Everything `DoGet` needs to run a query, carried by the client between `GetFlightInfo` and `DoGet`.
 */
@OptIn(InternalApi::class)
@Serializable
internal class QueryTicket(
    val dbName: DatabaseName,
    val sql: String,
    val params: QueryParams?,
    val basis: QueryBasis,
) {
    fun encode(): ByteString = ByteString.copyFromUtf8(Json.encodeToString(serializer(), this))

    companion object {
        fun decode(bytes: ByteString): QueryTicket =
            try {
                Json.decodeFromString(serializer(), bytes.toStringUtf8())
            } catch (e: SerializationException) {
                throw Incorrect("invalid ticket", "xtdb.flight-sql/invalid-ticket", cause = e)
            } catch (e: IllegalArgumentException) {
                throw Incorrect("invalid ticket", "xtdb.flight-sql/invalid-ticket", cause = e)
            }
    }
}
