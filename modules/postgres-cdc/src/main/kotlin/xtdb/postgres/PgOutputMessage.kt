package xtdb.postgres

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/**
 * Parsed pgoutput logical replication protocol messages.
 *
 * See: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
 */
sealed interface PgOutputMessage {

    data class Relation(
        val relationId: Int,
        val schema: String,
        val table: String,
        val replicaIdentity: ReplicaIdentity,
        val columns: List<Column>,
    ) : PgOutputMessage {
        data class Column(val name: String, val typeOid: Int)
    }

    enum class ReplicaIdentity(val code: Char) {
        DEFAULT('d'),
        NOTHING('n'),
        FULL('f'),
        INDEX('i');

        companion object {
            fun fromCode(c: Char): ReplicaIdentity =
                entries.find { it.code == c }
                    ?: throw UnsupportedOperationException("Unknown replica identity code: '$c'")
        }
    }

    data class Begin(val finalLsn: Long, val commitTimestamp: Long, val xid: Int) : PgOutputMessage
    data class Commit(val commitLsn: Long, val endLsn: Long, val commitTimestamp: Long) : PgOutputMessage
    data class Insert(val relationId: Int, val values: List<ColumnValue>) : PgOutputMessage

    data class Update(
        val relationId: Int, val oldValues: List<ColumnValue>?, val newValues: List<ColumnValue>
    ) : PgOutputMessage

    data class Delete(val relationId: Int, val oldValues: List<ColumnValue>) : PgOutputMessage

    /**
     * Column value in a data message.
     * - [Null] for SQL NULL
     * - [Text] for text-format values (the common case with pgoutput)
     * - [Unchanged] for TOASTed columns that didn't change
     */
    sealed interface ColumnValue {
        data object Null : ColumnValue
        data object Unchanged : ColumnValue
        data class Text(val value: String) : ColumnValue
    }

    companion object {
        fun parse(buf: ByteBuffer): PgOutputMessage =
            when (val type = buf.get().toInt().toChar()) {
                'R' -> parseRelation(buf)
                'B' -> parseBegin(buf)
                'C' -> parseCommit(buf)
                'I' -> parseInsert(buf)
                'U' -> parseUpdate(buf)
                'D' -> parseDelete(buf)
                else -> throw UnsupportedOperationException(
                    "Unknown pgoutput message type: '$type' (0x${type.code.toString(16)})"
                )
            }

        private fun parseRelation(buf: ByteBuffer): Relation {
            val relationId = buf.getInt()
            val schema = buf.readCString()
            val table = buf.readCString()
            val replicaIdentity = ReplicaIdentity.fromCode(buf.get().toInt().toChar())
            val numColumns = buf.getShort().toInt()
            val columns = (0 until numColumns).map {
                buf.get() // flags
                val name = buf.readCString()
                val typeOid = buf.getInt()
                buf.getInt() // type modifier
                Relation.Column(name, typeOid)
            }
            return Relation(relationId, schema, table, replicaIdentity, columns)
        }

        private fun parseBegin(buf: ByteBuffer): Begin {
            val finalLsn = buf.getLong()
            val commitTimestamp = buf.getLong()
            val xid = buf.getInt()
            return Begin(finalLsn, commitTimestamp, xid)
        }

        private fun parseCommit(buf: ByteBuffer): Commit {
            buf.get() // flags
            val commitLsn = buf.getLong()
            val endLsn = buf.getLong()
            val commitTimestamp = buf.getLong()
            return Commit(commitLsn, endLsn, commitTimestamp)
        }

        private fun parseInsert(buf: ByteBuffer): Insert {
            val relationId = buf.getInt()
            buf.get() // 'N' for new tuple
            val values = parseTupleData(buf)
            return Insert(relationId, values)
        }

        private fun parseUpdate(buf: ByteBuffer): Update {
            val relationId = buf.getInt()
            val marker = buf.get().toInt().toChar()

            val oldValues: List<ColumnValue>?
            val newMarker: Char

            // 'K' = old key, 'O' = old full row, 'N' = new tuple (no old data)
            if (marker == 'K' || marker == 'O') {
                oldValues = parseTupleData(buf)
                newMarker = buf.get().toInt().toChar()
            } else {
                oldValues = null
                newMarker = marker
            }

            check(newMarker == 'N') { "Expected 'N' marker for new tuple in UPDATE, got '$newMarker'" }
            val newValues = parseTupleData(buf)
            return Update(relationId, oldValues, newValues)
        }

        private fun parseDelete(buf: ByteBuffer): Delete {
            val relationId = buf.getInt()
            buf.get() // 'K' or 'O'
            val oldValues = parseTupleData(buf)
            return Delete(relationId, oldValues)
        }

        private fun parseTupleData(buf: ByteBuffer): List<ColumnValue> {
            val numColumns = buf.getShort().toInt()
            return (0 until numColumns).map {
                when (val colType = buf.get().toInt().toChar()) {
                    'n' -> ColumnValue.Null
                    'u' -> ColumnValue.Unchanged
                    't' -> {
                        val len = buf.getInt()
                        val bytes = ByteArray(len)
                        buf.get(bytes)
                        ColumnValue.Text(String(bytes, StandardCharsets.UTF_8))
                    }

                    else -> throw UnsupportedOperationException("Unknown column value type: '$colType'")
                }
            }
        }

        private fun ByteBuffer.readCString(): String {
            val start = position()
            while (get() != 0.toByte()) { /* scan for null terminator */
            }
            val len = position() - start - 1
            val bytes = ByteArray(len)
            position(start)
            get(bytes)
            get() // consume null terminator
            return String(bytes, StandardCharsets.UTF_8)
        }
    }
}
