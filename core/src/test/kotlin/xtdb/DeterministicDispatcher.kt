package xtdb

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Runnable
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random

class DeterministicDispatcher(private val rand: Random) : CoroutineDispatcher() {

    constructor(seed: Int) : this(Random(seed))

    private data class DispatchJob(val context: CoroutineContext, val block: Runnable)


    private val jobs = mutableListOf<DispatchJob>()

    @Volatile
    private var running = false

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        jobs.add(DispatchJob(context, block))

        if (!running) {
            running = true
            while (jobs.isNotEmpty()) {
                val idx = rand.nextInt(jobs.size)
                val job = jobs.removeAt(idx)
                job.block.run()
            }
            running = false
        }
    }
}