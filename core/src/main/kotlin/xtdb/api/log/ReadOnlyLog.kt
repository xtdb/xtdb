package xtdb.api.log

import xtdb.error.Incorrect

class ReadOnlyLog<M>(private val delegate: Log<M>) : Log<M> by delegate {
    override fun appendMessage(message: M) =
        throw Incorrect("Cannot append to read-only database log")

    override fun openAtomicProducer(transactionalId: String) =
        throw Incorrect("Cannot open atomic producer on read-only database log")
}
