package xtdb.api.log

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import xtdb.api.log.Log.Record
import java.nio.file.Path
import kotlin.time.Duration.Companion.seconds

class ReadOnlyLocalLogTest {

    @TempDir
    lateinit var tempDir: Path

    private fun txMessage(id: Byte) = SourceMessage.LegacyTx(byteArrayOf(-1, id))

    @Test
    fun `tailAll observes external writes at N=1`(): Unit = runBlocking {
        observesExternalWrites(partitions = 1)
    }

    @Test
    fun `tailAll observes external writes at N greater than 1`(): Unit = runBlocking {
        observesExternalWrites(partitions = 2)
    }

    // msg1's arrival via catch-up is a synchronisation barrier: it proves the reader has finished
    // its initial scan and is in the watch loop. msg2 can then only be delivered by the watcher.
    private suspend fun observesExternalWrites(partitions: Int) = coroutineScope {
        val root = tempDir.resolve("log")
        val factory = LocalLog.Factory(root)
        val readPartition = partitions - 1

        factory.openSourceLog(emptyMap(), partitions).use { writer ->
            writer.appendMessage(txMessage(1), readPartition)

            factory.openReadOnlySourceLog(emptyMap(), partitions).use { reader ->
                val ch = Channel<Record<SourceMessage>>(Channel.UNLIMITED)
                val job = launch(Dispatchers.IO) {
                    reader.tailAll(readPartition, -1L) { recs -> recs.forEach { ch.send(it) } }
                }
                try {
                    val msg1 = withTimeoutOrNull(3.seconds) { ch.receive() }
                    assertNotNull(msg1, "catch-up should deliver the pre-existing record")

                    writer.appendMessage(txMessage(2), readPartition)

                    val msg2 = withTimeoutOrNull(3.seconds) { ch.receive() }
                    assertNotNull(msg2, "watcher should deliver the post-subscription record")
                } finally {
                    job.cancelAndJoin()
                }
            }
        }
    }
}
