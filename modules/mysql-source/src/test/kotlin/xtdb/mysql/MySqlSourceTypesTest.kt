package xtdb.mysql

import io.kotest.assertions.nondeterministic.eventually
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import xtdb.api.Xtdb
import java.nio.file.Files
import kotlin.time.Duration.Companion.seconds

@Tag("integration")
class MySqlSourceTypesTest : MySqlSourceTestBase() {
    private lateinit var db: String
    private var node: Xtdb? = null

    @BeforeEach
    fun setUp() { db = unique("types"); mysqlExecute("CREATE DATABASE $db") }

    @AfterEach
    fun tearDown() { node?.close(); mysqlExecute("DROP DATABASE IF EXISTS $db") }

    private val columns = listOf(
        "c_tiny TINYINT" to "-5", "c_utiny TINYINT UNSIGNED" to "250", "c_small SMALLINT UNSIGNED" to "65535",
        "c_med MEDIUMINT UNSIGNED" to "16777215", "c_int INT UNSIGNED" to "4294967295", "c_big BIGINT" to "-9223372036854775808",
        "c_ubig BIGINT UNSIGNED" to "18446744073709551615", "c_bool BOOLEAN" to "1",
        "c_float FLOAT" to "1.1", "c_double DOUBLE" to "0.1", "c_dec DECIMAL(10,2)" to "1234.50", "c_negdec DECIMAL(20,5)" to "-0.00001",
        "c_char CHAR(4)" to "'ab'", "c_varchar VARCHAR(20)" to "'héllo 世界'", "c_latin1 VARCHAR(10) CHARACTER SET latin1" to "'café'",
        "c_text TEXT" to "'long text'", "c_bin BINARY(4)" to "x'6364'", "c_varbin VARBINARY(10)" to "x'00ff10'", "c_blob BLOB" to "x'deadbeef'",
        "c_enum ENUM('x','y','it''s')" to "'it''s'", "c_set SET('p','q','r')" to "'p,r'", "c_emptyset SET('p','q')" to "''",
        "c_bit1 BIT(1)" to "b'1'", "c_bit5 BIT(5)" to "b'10101'", "c_bit64 BIT(64)" to "b'1'",
        "c_date DATE" to "'2020-02-29'", "c_zerodate DATE" to "'0000-00-00'",
        "c_dt DATETIME" to "'2020-01-02 03:04:05'", "c_dt6 DATETIME(6)" to "'2020-01-02 03:04:05.123456'", "c_dt3 DATETIME(3)" to "'1999-12-31 23:59:59.999'",
        "c_ts TIMESTAMP NULL" to "'2021-02-03 04:05:06'", "c_ts6 TIMESTAMP(6) NULL" to "'2021-02-03 04:05:06.123456'",
        "c_time TIME" to "'12:34:56'", "c_time_neg TIME(3)" to "'-838:59:59.123'", "c_time_big TIME(6)" to "'100:00:00.000001'", "c_time_negfrac TIME(2)" to "'-00:00:00.50'",
        "c_year YEAR" to "2024", "c_json JSON" to """'{"a": [1, 2.5, "x", null], "b": {"c": true}, "d": 12345678901}'""", "c_jsonstr JSON" to "'\"hi\"'",
        "c_null INT" to "NULL", "c_uint_last INT UNSIGNED" to "4294967295",
    )

    @Test
    fun `snapshot and binlog agree on every column type`() = runTest(timeout = 180.seconds) {
        val cols = columns.joinToString { it.first }
        val names = columns.joinToString { it.first.substringBefore(' ') }
        val values = columns.joinToString { it.second }

        mysqlExecute(
            "SET time_zone = '+00:00'",
            "CREATE TABLE $db.t (_id INT PRIMARY KEY, $cols)",
        )
        mysqlConn().use { c ->
            c.createStatement().use { s ->
                s.execute("SET time_zone = '+00:00'")
                s.execute("SET sql_mode = ''")
                s.execute("INSERT INTO $db.t (_id, $names) VALUES (1, $values)")
            }
        }

        val tmp = Files.createTempDirectory("mysql-types")
        val node = openNode(tmp.resolve("log"), tmp.resolve("storage")).also { node = it }
        attachCdc(node, "cdc", tmp.resolve("cdc-log"), tmp.resolve("cdc-storage"), serverId(), listOf("$db.t"))
        awaitStreaming(node)

        mysqlConn().use { c ->
            c.createStatement().use { s ->
                s.execute("SET time_zone = '+00:00'")
                s.execute("SET sql_mode = ''")
                s.execute("INSERT INTO $db.t (_id, $names) VALUES (2, $values)")
            }
        }

        val rows = eventually(30.seconds) {
            xtQuery(node, "cdc", "SELECT * FROM $db.t ORDER BY _id").also { assertEquals(2, it.size) }
        }

        val (snap, stream) = rows
        val diffs = (snap.keys + stream.keys).filter { it != "_id" && !same(snap[it], stream[it]) }
            .map { "$it: snapshot=${show(snap[it])} binlog=${show(stream[it])}" }
        assertEquals(emptyList<String>(), diffs)
    }

    private fun show(v: Any?) = if (v == null) "null" else "${v.javaClass.simpleName}:" + (if (v is ByteArray) v.joinToString("") { "%02x".format(it) } else v.toString())
    private fun same(a: Any?, b: Any?) = if (a is ByteArray && b is ByteArray) a.contentEquals(b) else a == b
}
