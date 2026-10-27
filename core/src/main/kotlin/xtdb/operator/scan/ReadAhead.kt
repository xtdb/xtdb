package xtdb.operator.scan

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import xtdb.util.debug
import xtdb.util.logger

private val LOG = ReadAhead::class.logger

/**
 * Yields [items] in order, handing each to [readAhead], on a coroutine of its own, while it's within [maxDepth] items of
 * being yielded.
 *
 * The window starts empty and widens by one item for each item yielded.
 * A [readAhead] that fails is logged at debug and otherwise ignored.
 * [close] cancels the reads ahead and waits for any that can't stop mid-read.
 */
internal class ReadAhead<T>(
    private val items: Iterator<T>,
    private val maxDepth: Int,
    dispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val readAhead: suspend (T) -> Unit,
) : Iterator<T>, AutoCloseable {

    private val scope = CoroutineScope(SupervisorJob() + dispatcher)
    private val ahead = ArrayDeque<T>()
    private var depth = 0

    override fun hasNext() = ahead.isNotEmpty() || items.hasNext()

    override fun next(): T {
        val item = ahead.removeFirstOrNull() ?: items.next()

        while (ahead.size < depth && items.hasNext()) {
            val next = items.next().also { ahead.addLast(it) }
            scope.launch {
                try {
                    readAhead(next)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    LOG.debug(e, "read-ahead failed")
                }
            }
        }

        if (depth < maxDepth) depth++
        return item
    }

    override fun close() = runBlocking { scope.coroutineContext.job.cancelAndJoin() }
}
