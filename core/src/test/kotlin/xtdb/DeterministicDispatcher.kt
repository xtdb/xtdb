package xtdb

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Runnable
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random

class DeterministicDispatcher(private val rand: Random) : CoroutineDispatcher() {

    constructor(seed: Int) : this(Random(seed))

    private data class DispatchJob(val context: CoroutineContext, val block: Runnable)

    private val jobs = mutableListOf<DispatchJob>()

    private var runningThread: Thread? = null

    /** Total number of jobs picked from the queue. */
    var totalPicks = 0
        private set

    /** Number of picks where the queue had more than one job (i.e. a real interleaving choice). */
    var interleavedPicks = 0
        private set

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        check(runningThread == null || runningThread == Thread.currentThread()) {
            "Cross-thread dispatch from ${Thread.currentThread().name} while running on ${runningThread?.name}"
        }

        jobs.add(DispatchJob(context, block))

        if (runningThread == null) {
            runningThread = Thread.currentThread()
            while (jobs.isNotEmpty()) {
                totalPicks++
                if (jobs.size > 1) interleavedPicks++
                val idx = rand.nextInt(jobs.size)
                val job = jobs.removeAt(idx)
                job.block.run()
            }
            runningThread = null
        }
    }
}