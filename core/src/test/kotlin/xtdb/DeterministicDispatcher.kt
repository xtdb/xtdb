package xtdb

import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Runnable
import org.testcontainers.shaded.org.bouncycastle.asn1.cmp.Challenge
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random

class DeterministicDispatcher(private val rand: Random) : CoroutineDispatcher() {

    constructor(seed: Int) : this(Random(seed))

    private data class DispatchJob(val context: CoroutineContext, val block: Runnable)


    private val jobs = mutableSetOf<DispatchJob>()

    @Volatile
    private var running = false

    override fun dispatch(context: CoroutineContext, block: Runnable) {
        jobs.add(DispatchJob(context, block))

        if (!running) {
            running = true
            while (true) {
                val job = jobs.randomOrNull(rand) ?: break
                jobs.remove(job)
                job.block.run()
            }
            running = false
        }
    }
}