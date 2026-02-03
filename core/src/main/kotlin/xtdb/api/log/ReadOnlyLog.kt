package xtdb.api.log

import xtdb.error.Incorrect

class ReadOnlyLog(private val delegate: Log) : Log by delegate {
    override fun appendMessage(message: Log.Message) =
        throw Incorrect("Cannot append to read-only database log")

    override fun openAtomicProducer(transactionalId: String) =
        throw Incorrect("Cannot open atomic producer on read-only database log")
}
