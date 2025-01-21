package xtdb.api.log

import java.util.concurrent.CompletableFuture

interface FileLog {
    fun appendFileNotification(notification: Notification): CompletableFuture<Unit>
    fun subscribeFileNotifications(subscriber: Subscriber)

    interface Notification

    interface Subscriber {
        fun onSubscribe(closeHook: AutoCloseable)
        fun accept(record: Notification)
    }

    companion object {
        @JvmField
        val SOLO = object : FileLog {
            private var sub: Subscriber? = null

            override fun appendFileNotification(notification: Notification): CompletableFuture<Unit> {
                sub?.accept(notification)
                return CompletableFuture.completedFuture(Unit)
            }

            override fun subscribeFileNotifications(subscriber: Subscriber) {
                sub = subscriber
                subscriber.onSubscribe { sub = null }
            }
        }
    }
}
