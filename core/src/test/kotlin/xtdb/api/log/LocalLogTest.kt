package xtdb.api.log

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.RepeatedTest
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.io.TempDir
import xtdb.api.log.Log.Message
import xtdb.api.log.Log.Record
import java.nio.file.Path
import kotlin.time.Duration.Companion.seconds

class LocalLogTest {

    @TempDir
    lateinit var tempDir: Path

    @Tag("integration")
    @RepeatedTest(5000)
    fun `close should cancel all subscription coroutines without leaking`() = runTest(timeout = 10.seconds) {
        val log = LocalLog.Factory(tempDir.resolve("log")).openLog(emptyMap())

        // Create a subscription
        val records = mutableListOf<Record>()
        val subscription = log.subscribe(
            object : Log.Subscriber {
                override val latestProcessedMsgId: Long = -1
                override val latestSubmittedMsgId: Long = -1
                override fun processRecords(recs: List<Record>) {
                    recs.forEach { records.add(it) }
                }
            },
            -1
        )

        log.appendMessage(Message.FlushBlock(1))

        subscription.close()

        log.close()
    }
}