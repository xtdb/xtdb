package xtdb.api.log

import xtdb.api.error.Incorrect

/** @suppress */
class ReadOnlyLog<M>(private val delegate: Log<M>) : Log<M> by delegate {
    override suspend fun appendMessage(message: M, partition: Int): Log.MessageMetadata =
        throw Incorrect("Cannot append to read-only database log")

    override fun appendMessageBlocking(message: M, partition: Int): Nothing =
        throw Incorrect("Cannot append to read-only database log")
}
