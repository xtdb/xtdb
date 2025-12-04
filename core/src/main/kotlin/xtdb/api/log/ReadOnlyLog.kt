package xtdb.api.log

import xtdb.error.Incorrect

/**
 * A wrapper around a Log that prevents write operations.
 * Used for read-only database attachments.
 */
class ReadOnlyLog(private val delegate: Log) : Log by delegate {
    override fun appendMessage(message: Log.Message) =
        throw Incorrect("Cannot append to read-only database log")
}
