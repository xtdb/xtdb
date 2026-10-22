package xtdb.mysql

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.Test
import xtdb.api.error.Fault
import kotlinx.coroutines.flow.MutableStateFlow
import xtdb.api.tx.BlockDetails
import xtdb.api.tx.ExternalSourceToken
import xtdb.api.tx.OpenTx
import xtdb.api.tx.TxIndexer
import xtdb.api.tx.TxIndexer.TxResult
import java.time.Instant
import xtdb.mysql.proto.mySqlSourceToken

class MySqlSourceTest {

    private object UnusedDriver : MySqlDriver {
        override fun checkUpstream() = Unit
        override fun openSnapshot() = error("never snapshots")
        override suspend fun openStream(from: GtidSet) = error("never streams")
        override fun queryExecutedGtids() = error("never queried")
        override fun close() = Unit
    }

    private object UnusedIndexer : TxIndexer {
        override val latestBlock = MutableStateFlow<BlockDetails?>(null)
        override suspend fun executeTx(externalSourceToken: ExternalSourceToken?, systemTime: Instant?, writer: suspend (OpenTx) -> TxResult) = error("never indexes")
        override suspend fun submitTx(externalSourceToken: ExternalSourceToken?, systemTime: Instant?, writer: suspend (OpenTx) -> TxResult) = error("never indexes")
    }

    @Test
    fun `a token from an unfinished snapshot leaves the database inoperable`() = runTest {
        val token = mySqlSourceToken { gtidSet = ""; snapshotCompleted = false }.toByteArray()

        val e = assertThrows<Fault> {
            MySqlSource("db", "mysql", UnusedDriver, DirectMirror()).onPartitionAssigned(0, token, UnusedIndexer)
        }

        assertEquals("xtdb.mysql/incomplete-snapshot", e.errorCode())
    }
}
