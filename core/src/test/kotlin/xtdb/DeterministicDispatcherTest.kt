package xtdb

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.CountDownLatch
import kotlin.coroutines.EmptyCoroutineContext

class DeterministicDispatcherTest {

    @Test
    fun `cross-thread dispatch while running throws`() {
        val dispatcher = DeterministicDispatcher(seed = 42)
        val latch = CountDownLatch(1)
        var crossThreadError: Throwable? = null

        dispatcher.dispatch(EmptyCoroutineContext, Runnable {
            // We're inside the drain loop on the main thread.
            // Dispatch from a different thread — should fail.
            val thread = Thread {
                crossThreadError = assertThrows<IllegalStateException> {
                    dispatcher.dispatch(EmptyCoroutineContext, Runnable {})
                }
                latch.countDown()
            }
            thread.start()
            latch.await()
        })

        assert(crossThreadError != null) { "Expected cross-thread dispatch to throw" }
        assert("Cross-thread dispatch" in crossThreadError!!.message!!)
    }

    @Test
    fun `re-entrant dispatch from same thread succeeds`() {
        val dispatcher = DeterministicDispatcher(seed = 42)
        var innerRan = false

        dispatcher.dispatch(EmptyCoroutineContext, Runnable {
            dispatcher.dispatch(EmptyCoroutineContext, Runnable {
                innerRan = true
            })
        })

        assert(innerRan) { "Re-entrant dispatch should execute" }
    }
}
