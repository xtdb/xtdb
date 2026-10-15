package xtdb.api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.api.Xtdb.ExecutedTx
import xtdb.api.error.Fault
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException

class ExecutedTxFutureTest {

    private val looked = ExecutedTx(TransactionKey(4, Instant.EPOCH, Instant.EPOCH), true, null)

    @Test
    fun `a result the watchers had already moved past is looked up when read`() {
        val result = CompletableFuture<TransactionResult?>()
        val future = ExecutedTxFuture(result) { looked }

        result.complete(null)

        assertEquals(looked, future.get())
    }

    @Test
    fun `a result the watchers hand back isn't looked up`() {
        val res = TransactionResult.Committed(TransactionKey(4, Instant.EPOCH, Instant.EPOCH))
        val future = ExecutedTxFuture(CompletableFuture.completedFuture(res)) { error("looked up") }

        assertEquals(ExecutedTx(TransactionKey(4, Instant.EPOCH, Instant.EPOCH), true, null), future.get())
    }

    @Test
    fun `a failed look-up fails the read`() {
        val missing = Fault("tx 4 not found after await", "xtdb/tx-await-missing")
        val future = ExecutedTxFuture(CompletableFuture.completedFuture(null)) { throw missing }

        assertThrows<ExecutionException> { future.get() }
            .also { assertEquals(missing, it.cause) }
    }
}
