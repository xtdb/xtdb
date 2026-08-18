package xtdb.catalog

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.XtdbInternal
import xtdb.api.TableRef
import xtdb.api.Xtdb
import xtdb.arrow.VectorType
import xtdb.arrow.VectorType.Companion.I64
import xtdb.arrow.VectorType.Companion.UTF8
import xtdb.arrow.VectorType.Companion.fromLegs
import xtdb.arrow.VectorType.Companion.maybe
import xtdb.test.flushBlock
import xtdb.trie.ColumnName

/**
 * The historical⊔live join, asserted directly rather than through a query.
 *
 * A query can't catch a defect on either side, because the two halves cover for each other - and nils drop
 * out of Clojure row maps, so the emitted rows look right while the declared type is wrong.
 */
class ColumnTypesTest {

    private lateinit var xtdb: Xtdb

    @BeforeEach
    fun setUp() {
        xtdb = Xtdb.openNode { server { port = 0 } }
    }

    @AfterEach
    fun tearDown() = xtdb.close()

    private fun exec(sql: String) =
        xtdb.connect().use { conn -> conn.createStatement().use { it.setSqlQuery(sql); it.executeUpdate() } }

    private fun typeOf(table: String, vararg cols: ColumnName): Map<ColumnName, VectorType> {
        val db = (xtdb as XtdbInternal).dbCatalog.primary
        return db.openSnapshot(db.currentBasis()).use { dbSnap ->
            dbSnap.partitions.first().columnTypes(TableRef("public", table), cols.toList())
        }
    }

    @Test
    fun `unions types that disagree across the boundary`() {
        exec("INSERT INTO t RECORDS {_id: 1, v: 1}")
        xtdb.flushBlock()
        exec("INSERT INTO t RECORDS {_id: 2, v: 'hello'}")

        assertEquals(
            mapOf("v" to fromLegs(I64, UTF8)), typeOf("t", "v"),
            "both legs are real; neither side may overwrite the other"
        )
    }

    @Test
    fun `widens when the live rows lack a column the historical rows have`() {
        exec("INSERT INTO t RECORDS {_id: 1, v: 1}")
        xtdb.flushBlock()
        exec("INSERT INTO t RECORDS {_id: 2}")

        assertEquals(mapOf("v" to maybe(I64)), typeOf("t", "v"))
    }

    @Test
    fun `widens when the historical rows lack a column the live rows have`() {
        exec("INSERT INTO t RECORDS {_id: 1}")
        xtdb.flushBlock()
        exec("INSERT INTO t RECORDS {_id: 2, v: 1}")

        assertEquals(
            mapOf("v" to maybe(I64)), typeOf("t", "v"),
            "the mirror of the above - the historical side has rows that read null, so it contributes Null, not nothing"
        )
    }

    @Test
    fun `a column present nowhere still has a type`() {
        exec("INSERT INTO t RECORDS {_id: 1}")

        assertEquals(
            mapOf("nope" to VectorType.Null), typeOf("t", "nope"),
            "every row of a non-empty table reads null for it - asking is not a lookup miss"
        )
    }

    /**
     * `CREATE TABLE` on a populated table leaves a live segment holding only the newly-declared column and
     * no rows. It contributes nothing about `v`, so it must contribute `Nothing` — a segment with no rows
     * has no row that reads null.
     */
    @Test
    fun `a zero-row live segment does not make other columns nullable`() {
        exec("INSERT INTO t RECORDS {_id: 1, v: 1}")
        xtdb.flushBlock()
        exec("CREATE TABLE t (w)")

        assertEquals(
            mapOf("v" to I64), typeOf("t", "v"),
            "`v` is non-null in every block and segment; an empty segment lacking it must not widen it"
        )
    }

    /** The declaring segment holds no rows, but the table does, and they read null for `w`. */
    @Test
    fun `a column declared on a populated table is nullable, not the bottom`() {
        exec("INSERT INTO t RECORDS {_id: 1, v: 1}")
        xtdb.flushBlock()
        exec("CREATE TABLE t (w)")

        assertEquals(mapOf("w" to VectorType.Null), typeOf("t", "w"))
    }

    @Test
    fun `a column declared on an empty table is the bottom`() {
        exec("CREATE TABLE t (w)")

        assertEquals(mapOf("w" to VectorType.Nothing), typeOf("t", "w"))
    }

    /**
     * The op-type axis. Every case above is built from puts, which is how a source's row count came to stand
     * in for "rows that could carry a column" - `delete` and `erase` are rows too, and they carry none, so a
     * source made only of them contributes `Nothing`. They also never materialise a put leg, which is what
     * lets [xtdb.arrow.VectorType.absentContribution] tell them apart from a source that does have puts.
     */
    @Test
    fun `a delete-only live segment does not make columns nullable`() {
        exec("INSERT INTO t RECORDS {_id: 1, v: 1}")
        xtdb.flushBlock()
        exec("DELETE FROM t WHERE _id = 1")

        assertEquals(
            mapOf("v" to I64), typeOf("t", "v"),
            "the segment holds one delete and no put, so it has no row that could carry `v`"
        )
    }

    @Test
    fun `an erase-only live segment does not make columns nullable`() {
        exec("INSERT INTO t RECORDS {_id: 1, v: 1}")
        xtdb.flushBlock()
        exec("ERASE FROM t WHERE _id = 1")

        assertEquals(mapOf("v" to I64), typeOf("t", "v"))
    }

    /**
     * `v` is in the put's doc, so the lookup hits and the absent-column rule never runs — unlike the
     * delete-only case above, which is where [xtdb.arrow.VectorType.absentContribution] earns its keep.
     * Worth pinning even so: a delete sharing the segment must not disturb a recorded type. Note the
     * catalog could not narrow one in any case — types only ever widen, and the deleted row's `v` stays
     * queryable through history.
     */
    @Test
    fun `a put alongside a delete keeps the recorded type`() {
        exec("INSERT INTO t RECORDS {_id: 1, v: 1}")
        xtdb.flushBlock()
        exec("INSERT INTO t RECORDS {_id: 2, v: 2}")
        exec("DELETE FROM t WHERE _id = 1")

        assertEquals(mapOf("v" to I64), typeOf("t", "v"))
    }

    @Test
    fun `a table nobody has written is the lattice bottom`() {
        exec("INSERT INTO t RECORDS {_id: 1}")

        assertEquals(
            mapOf("v" to VectorType.Nothing), typeOf("no_such_table", "v"),
            "no rows anywhere, so nothing to contribute - Nothing is absorbed by any join it takes part in"
        )
    }
}
