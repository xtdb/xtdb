package xtdb.jdbc

import clojure.lang.Keyword
import org.postgresql.core.BaseConnection
import org.postgresql.jdbc.PgConnection
import org.postgresql.util.PGobject
import xtdb.time.*
import xtdb.types.RegClass
import xtdb.types.RegProc
import xtdb.types.ZonedDateTimeRange
import xtdb.util.TransitFormat.JSON
import xtdb.util.readTransit
import xtdb.util.requiringResolve
import java.net.URI
import java.sql.*
import java.time.*
import java.time.temporal.Temporal
import java.time.temporal.TemporalAccessor
import java.util.*
import java.util.Date
import xtdb.decode as decodeJson

internal class XtConnection(private val conn: PgConnection) : BaseConnection by conn {

    internal class XtResultSet(private val inner: ResultSet) : ResultSet by inner {
        private fun getPgObject(columnIndex: Int) = inner.getObject(columnIndex, PGobject::class.java)?.value

        override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp {
            assert(cal == null) { "calendar supplied but ignored" }
            return when (inner.metaData.getColumnTypeName(columnIndex)) {
                "timestamp" -> Timestamp.valueOf(getObject(columnIndex, LocalDateTime::class.java))
                "timestamptz" -> Timestamp.from(getObject(columnIndex, Instant::class.java))

                else -> inner.getTimestamp(columnIndex, cal)
            }
        }

        override fun getTimestamp(columnIndex: Int): Timestamp = getTimestamp(columnIndex, null)
        override fun getTimestamp(columnLabel: String) = getTimestamp(findColumn(columnLabel))
        override fun getTimestamp(columnLabel: String?, cal: Calendar?) = getTimestamp(findColumn(columnLabel), cal)

        override fun getObject(columnIndex: Int): Any? =
            when (inner.metaData.getColumnTypeName(columnIndex)) {
                "transit" -> getPgObject(columnIndex)?.encodeToByteArray()?.let { readTransit(it, JSON) }
                "keyword" -> getPgObject(columnIndex)?.let { Keyword.intern(it) }
                "json", "jsonb" -> getPgObject(columnIndex)?.let { decodeJson(it) }
                "date" -> getObject(columnIndex, LocalDate::class.java)
                "timestamp" -> getObject(columnIndex, LocalDateTime::class.java)
                "timestamptz" -> getObject(columnIndex, ZonedDateTime::class.java)
                "tstz-range" -> getObject(columnIndex, ZonedDateTimeRange::class.java)
                "interval" -> getObject(columnIndex, Interval::class.java)
                "regclass" -> getObject(columnIndex, RegClass::class.java)
                "regproc" -> getObject(columnIndex, RegProc::class.java)

                else -> inner.getObject(columnIndex)
            }

        override fun getObject(columnLabel: String): Any? = getObject(findColumn(columnLabel))

        override fun getObject(columnIndex: Int, map: Map<String, Class<*>>?): Any? =
            if (map.isNullOrEmpty()) getObject(columnIndex)
            else throw SQLFeatureNotSupportedException("getObject(Int, Map)")

        override fun getObject(columnLabel: String, map: Map<String, Class<*>>?): Any? =
            getObject(findColumn(columnLabel), map)

        override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>): T? =
            type.cast(
                getString(columnIndex)
                    ?.let { s ->
                        when (type) {
                            Instant::class.java -> s.asInstant()
                            OffsetDateTime::class.java -> s.asOffsetDateTime()
                            ZonedDateTime::class.java -> s.asZonedDateTime()
                            LocalDateTime::class.java -> s.asLocalDateTime()
                            Interval::class.java -> s.asInterval()
                            ZonedDateTimeRange::class.java -> s.asZonedDateTimeRange()
                            RegClass::class.java -> RegClass(s.toInt())
                            RegProc::class.java -> RegProc(s.toInt())

                            else -> inner.getObject(columnIndex, type)
                        }
                    }
            )

        override fun <T : Any?> getObject(columnLabel: String?, type: Class<T>): T = inner.getObject(columnLabel, type)
    }

    internal inner class XtStatement(private val inner: Statement) : Statement by inner {
        override fun executeQuery(sql: String) = XtResultSet(inner.executeQuery(sql))
        override fun getResultSet() = XtResultSet(inner.resultSet)
        override fun getConnection() = this@XtConnection
    }

    internal inner class XtCallableStatement(private val inner: CallableStatement) : CallableStatement by inner

    internal inner class XtPreparedStatement(private val inner: PreparedStatement) : PreparedStatement by inner {
        override fun getResultSet() = inner.resultSet?.let { XtResultSet(it) }

        override fun executeQuery() = XtResultSet(inner.executeQuery())
        override fun executeQuery(sql: String) = XtResultSet(inner.executeQuery(sql))
        override fun getGeneratedKeys() = XtResultSet(inner.generatedKeys)

        private fun setTransit(parameterIndex: Int, x: Any?) {
            inner.setObject(
                parameterIndex,
                requiringResolve("xtdb.serde/->pg-obj").invoke(x)
            )
        }

        private fun TemporalAccessor?.toSqlString(): String? =
            this?.let { SQL_TIMESTAMP_FORMATTER.format(it) }

        private fun setIsoTimestampTz(parameterIndex: Int, x: Temporal) {
            inner.setObject(
                parameterIndex,
                PGobject().also {
                    it.type = "timestamptz"
                    it.value = x.toSqlString()
                }
            )
        }

        private fun setIsoTimestamp(parameterIndex: Int, x: Temporal) {
            inner.setObject(
                parameterIndex,
                PGobject().also {
                    it.type = "timestamp"
                    it.value = x.toSqlString()
                }
            )
        }

        private val ZonedDateTimeRange.asPgObject
            get() = PGobject().also {
                it.type = "tstz-range"
                it.value = "[${from.toSqlString() ?: ""},${to.toSqlString() ?: ""})"
            }

        private val Interval.asPgObject
            get() = PGobject().also {
                it.type = "interval"
                it.value = toString()
            }

        override fun setObject(parameterIndex: Int, x: Any?) {
            when (x) {
                is Map<*, *>, is List<*>, is Set<*>, is Keyword, is URI ->
                    setTransit(parameterIndex, x)

                is ZonedDateTime -> setIsoTimestampTz(parameterIndex, x)
                is Instant -> setIsoTimestampTz(parameterIndex, x)
                is ZonedDateTimeRange -> inner.setObject(parameterIndex, x.asPgObject)
                is Interval -> inner.setObject(parameterIndex, x.asPgObject)
                is Date -> setObject(parameterIndex, x.toInstant().atZone(ZoneOffset.UTC).toLocalDateTime())
                is LocalDateTime -> setIsoTimestamp(parameterIndex, x)

                else -> inner.setObject(parameterIndex, x)
            }
        }

        override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int) {
            setObject(parameterIndex, x, targetSqlType, -1)
        }

        override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: Int, scaleOrLength: Int) {
            if (targetSqlType == Types.OTHER)
                when (x) {
                    is Map<*, *>, is List<*>, is Set<*>, is Keyword, is URI ->
                        setTransit(parameterIndex, x)

                    is ZonedDateTime -> setIsoTimestampTz(parameterIndex, x)
                    is Instant -> setIsoTimestampTz(parameterIndex, x)

                    is ZonedDateTimeRange ->
                        inner.setObject(parameterIndex, x.asPgObject, targetSqlType, scaleOrLength)

                    is Interval ->
                        inner.setObject(parameterIndex, x.asPgObject, targetSqlType, scaleOrLength)

                    else -> inner.setObject(parameterIndex, x, targetSqlType, scaleOrLength)
                }
            else inner.setObject(parameterIndex, x, targetSqlType, scaleOrLength)
        }

        override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: SQLType?) {
            setObject(parameterIndex, x, targetSqlType, -1)
        }

        override fun setObject(parameterIndex: Int, x: Any?, targetSqlType: SQLType?, scaleOrLength: Int) {
            throw SQLFeatureNotSupportedException("setObject(int, Object, SQLType, int)")
        }
    }

    override fun createStatement() = XtStatement(conn.createStatement())

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int) =
        XtStatement(conn.createStatement(resultSetType, resultSetConcurrency))

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int) =
        XtStatement(conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability))

    override fun prepareCall(sql: String) = XtCallableStatement(conn.prepareCall(sql))

    override fun prepareCall(sql: String, resultSetType: Int, resultSetConcurrency: Int) =
        XtCallableStatement(conn.prepareCall(sql, resultSetType, resultSetConcurrency))

    override fun prepareCall(
        sql: String, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int
    ) = XtCallableStatement(conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability))

    override fun prepareStatement(sql: String) = XtPreparedStatement(conn.prepareStatement(sql))

    override fun prepareStatement(sql: String, autoGeneratedKeys: Int) =
        XtPreparedStatement(conn.prepareStatement(sql, autoGeneratedKeys))

    override fun prepareStatement(sql: String, columnIndexes: IntArray?) =
        XtPreparedStatement(conn.prepareStatement(sql, columnIndexes))

    override fun prepareStatement(sql: String, columnNames: Array<out String>?) =
        XtPreparedStatement(conn.prepareStatement(sql, columnNames))

    override fun prepareStatement(sql: String, resultSetType: Int, resultSetConcurrency: Int) =
        XtPreparedStatement(conn.prepareStatement(sql, resultSetType, resultSetConcurrency))

    override fun prepareStatement(
        sql: String, resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int
    ) = XtPreparedStatement(conn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability))
}