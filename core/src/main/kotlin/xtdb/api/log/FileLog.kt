package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.future.future
import xtdb.api.storage.ObjectStore
import java.util.concurrent.CompletableFuture

interface FileLog : AutoCloseable {

    data class Notification(val added: Collection<ObjectStore.StoredObject>)

    companion object {
        @JvmStatic
        fun openInMemory() = InMemory()
    }

    fun appendFileNotification(notification: Notification): CompletableFuture<Unit>
    fun subscribeToFileNotifications(processor: Processor): AutoCloseable

    @FunctionalInterface
    fun interface Processor {
        fun processNotification(notification: Notification)
    }

    @OptIn(ExperimentalCoroutinesApi::class, DelicateCoroutinesApi::class)
    class InMemory : FileLog {
        @Volatile
        private var processor: Processor? = null

        private val scope: CoroutineScope = CoroutineScope(newSingleThreadContext("log"))

        override fun appendFileNotification(notification: Notification): CompletableFuture<Unit> =
            scope.future {
                processor?.processNotification(notification)
            }

        @Synchronized
        override fun subscribeToFileNotifications(processor: Processor): AutoCloseable {
            check(this.processor == null) { "Only one file notification processor can be subscribed" }

            this.processor = processor

            return AutoCloseable {
                synchronized(this@InMemory) {
                    this.processor = null
                }
            }
        }

        override fun close() {
            runBlocking { scope.coroutineContext.job.cancelAndJoin() }
        }
    }
}