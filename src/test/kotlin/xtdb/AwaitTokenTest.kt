package xtdb

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.database.decodeTxBasisToken
import xtdb.database.encodeTxBasisToken

class AwaitTokenTest {

    private fun AwaitToken.decoded() = value?.decodeTxBasisToken()

    @Test
    fun `record folds a committed tx into the token`() {
        val at = AwaitToken()
        at.record("mydb", 7)
        assertEquals(mapOf("mydb" to listOf(7L)), at.decoded())
    }

    @Test
    fun `successive records on the same db keep the latest`() {
        val at = AwaitToken()
        at.record("mydb", 4)
        at.record("mydb", 9)
        assertEquals(mapOf("mydb" to listOf(9L)), at.decoded())
    }

    @Test
    fun `records across dbs merge - the attach-detach case`() {
        val at = AwaitToken()
        at.record("secondary", 2)
        at.record("xtdb", 5)
        assertEquals(mapOf("secondary" to listOf(2L), "xtdb" to listOf(5L)), at.decoded())
    }

    @Test
    fun `value can be replaced directly - for SET AWAIT_TOKEN`() {
        val at = AwaitToken()
        at.record("mydb", 1)
        at.value = mapOf("mydb" to listOf(42L)).encodeTxBasisToken()
        assertEquals(mapOf("mydb" to listOf(42L)), at.decoded())
    }
}
