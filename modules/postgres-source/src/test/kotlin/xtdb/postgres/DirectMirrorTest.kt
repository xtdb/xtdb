package xtdb.postgres

import clojure.lang.Keyword
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.error.Unsupported
import xtdb.api.tx.OpenTx
import xtdb.table.PrimaryKey
import java.time.Instant

class DirectMirrorTest {

    @Test
    fun `a table keyed on another column is rejected rather than written by _id`() {
        val openTxTable = mockk<OpenTx.Table>()
        every { openTxTable.primaryKey } returns PrimaryKey(listOf("a"))

        val openTx = mockk<OpenTx>()
        every { openTx.table(any(), any()) } returns openTxTable

        val tx = PostgresDriver.Transaction(
            0, Instant.EPOCH, listOf(RowOp.Put("public", "users", mapOf("_id" to 1L, "a" to 2L))),
        )

        val e = assertThrows<Unsupported> { DirectMirror().indexTx(tx, openTx) }
        assertEquals(Keyword.intern("xtdb.postgres", "table-not-keyed-on-id"), e.data.valAt(Keyword.intern("xtdb.error", "code")))
    }
}
