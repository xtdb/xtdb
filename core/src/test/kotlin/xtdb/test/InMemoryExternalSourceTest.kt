package xtdb.test

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import xtdb.api.TransactionKey
import xtdb.api.TransactionResult
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer
import xtdb.api.tx.TxIndexer.TxResult
import java.time.Instant

class InMemoryExternalSourceTest {

    private class RecordingIndexer : TxIndexer {
        val tokens = Channel<ExternalSourceToken?>(Channel.UNLIMITED)

        override val latestBlock get() = error("unused")

        override suspend fun executeTx(
            externalSourceToken: ExternalSourceToken?, systemTime: Instant?, writer: suspend (OpenTx) -> TxResult
        ): TransactionResult {
            tokens.send(externalSourceToken)
            return TransactionResult.Committed(TransactionKey(0, Instant.EPOCH))
        }

        override suspend fun submitTx(
            externalSourceToken: ExternalSourceToken?, systemTime: Instant?, writer: suspend (OpenTx) -> TxResult
        ): Deferred<TransactionResult> = CompletableDeferred(executeTx(externalSourceToken, systemTime, writer))
    }

    @Test
    fun `a term resumes just after the token it is handed`() = runTest {
        val source = InMemoryExternalSource()
        val first = source.publish()
        val second = source.publish()
        val third = source.publish()

        val indexer = RecordingIndexer()
        val term = backgroundScope.launch { source.open().onPartitionAssigned(0, first, indexer) }

        assertEquals(listOf(second, third).map { it.toList() }, List(2) { indexer.tokens.receive()!!.toList() })
        term.cancel()
    }

    @Test
    fun `every source opened over one upstream sees every message`() = runTest {
        val source = InMemoryExternalSource()
        val a = RecordingIndexer()
        val b = RecordingIndexer()

        backgroundScope.launch { source.open().onPartitionAssigned(0, null, a) }
        backgroundScope.launch { source.open().onPartitionAssigned(0, null, b) }

        val tokens = List(2) { source.publish().toList() }

        assertEquals(tokens, List(2) { a.tokens.receive()!!.toList() })
        assertEquals(tokens, List(2) { b.tokens.receive()!!.toList() })
    }
}
