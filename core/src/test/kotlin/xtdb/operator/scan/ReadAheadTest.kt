package xtdb.operator.scan

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class ReadAheadTest {

    @Test
    fun `the window widens by an item for each item yielded, up to its depth`() {
        val readAhead = mutableListOf<Int>()
        val yielded = mutableListOf<Int>()

        ReadAhead((0 until 10).iterator(), 3, Dispatchers.Unconfined) { readAhead += it }.use { items ->
            for (item in items) {
                assertEquals((2..minOf(item + minOf(item, 3), 9)).toList(), readAhead, "yielding $item")
                yielded += item
            }
        }

        assertEquals((0 until 10).toList(), yielded)
    }

    @Test
    fun `a read ahead that fails doesn't fail the iteration`() {
        val items = ReadAhead((0 until 10).iterator(), 3, Dispatchers.Unconfined) { error("boom") }
        assertEquals((0 until 10).toList(), items.use { it.asSequence().toList() })
    }

    @Test
    fun `closing cancels the reads ahead and waits for one that can't stop`() {
        val started = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val finished = AtomicBoolean()

        val items = ReadAhead((0 until 10).iterator(), 4) { item ->
            if (item == 2) {
                started.complete(Unit)
                withContext(NonCancellable) { release.await() }
                finished.set(true)
            } else awaitCancellation()
        }

        repeat(3) { items.next() }
        runBlocking { started.await() }

        val closing = thread { items.close() }
        closing.join(200)
        assertTrue(closing.isAlive, "close waits for the read in flight")

        release.complete(Unit)
        closing.join()
        assertTrue(finished.get())
    }
}
