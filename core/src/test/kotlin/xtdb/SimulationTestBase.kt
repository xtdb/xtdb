package xtdb

import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.TestTemplate
import org.junit.jupiter.api.extension.TestTemplateInvocationContext
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider
import xtdb.util.logger
import xtdb.util.warn
import java.util.stream.Stream
import kotlin.random.Random

private val DEFAULT_ITERATIONS: Int = System.getProperty("xtdb.simulation-test-iterations", "100").toInt()

private val ExtensionContext.explicitSeed get() = requiredTestMethod.getAnnotation(WithSeed::class.java)?.seed

@TestTemplate
@ExtendWith(RepeatableSimulationTest.InvocationContextProvider::class)
annotation class RepeatableSimulationTest(val iterations: Int = -1) {

    object InvocationContext : TestTemplateInvocationContext {
        override fun getDisplayName(invocationIndex: Int) = "[iteration $invocationIndex]"
    }

    class InvocationContextProvider : TestTemplateInvocationContextProvider {
        override fun supportsTestTemplate(ctx: ExtensionContext) =
            ctx.requiredTestMethod.isAnnotationPresent(RepeatableSimulationTest::class.java)

        private val ExtensionContext.explicitIterations
            get() =
                requiredTestMethod.getAnnotation(RepeatableSimulationTest::class.java)
                    .iterations
                    .takeIf { it >= 0 }

        override fun provideTestTemplateInvocationContexts(ctx: ExtensionContext): Stream<TestTemplateInvocationContext> {
            val iterations = ctx.explicitIterations ?: if (ctx.explicitSeed != null) 1 else DEFAULT_ITERATIONS
            return List<TestTemplateInvocationContext>(iterations) { InvocationContext }.stream()
        }
    }
}

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class WithSeed(val seed: Int)

class SeedExtension : BeforeEachCallback, TestExecutionExceptionHandler {
    override fun beforeEach(ctx: ExtensionContext) {
        val explicitSeed = ctx.explicitSeed

        ctx.requiredTestInstance.let { it as SimulationTestBase }
            .also {
                val seed = explicitSeed ?: Random.nextInt()
                it.currentSeed = seed
                it.rand = Random(seed)
                it.dispatcher = DeterministicDispatcher(seed)
            }
    }

    override fun handleTestExecutionException(context: ExtensionContext, throwable: Throwable) {
        val testInstance = context.testInstance.orElse(null)
        if (testInstance is SimulationTestBase) {
            val seed = testInstance.currentSeed
            val logger = testInstance::class.logger
            logger.warn(throwable, "Test failed with seed: ${testInstance.currentSeed}")
            throw AssertionError("Test threw an exception (seed=$seed)", throwable)
        }
    }
}

@ExtendWith(SeedExtension::class)
abstract class SimulationTestBase {
    var currentSeed: Int = 0
    lateinit var rand: Random
    lateinit var dispatcher: DeterministicDispatcher

    protected fun assertInterleaved() {
        assertTrue(
            dispatcher.interleavedPicks > 0,
            "Dispatcher never had multiple concurrent jobs — simulation did not interleave (seed=$currentSeed)"
        )
    }
}