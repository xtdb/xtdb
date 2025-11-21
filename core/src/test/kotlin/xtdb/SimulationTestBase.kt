package xtdb

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import xtdb.util.logger
import xtdb.util.warn
import java.util.concurrent.TimeUnit
import kotlin.random.Random

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class WithSeed(val seed: Int)

class SeedExtension : BeforeEachCallback {
    override fun beforeEach(context: ExtensionContext) {
        val annotation = context.requiredTestMethod
            .getAnnotation(WithSeed::class.java)

        annotation?.let { withSeed ->
            context.requiredTestInstance.let { testInstance ->
                if (testInstance is SimulationTestBase) {
                    testInstance.explicitSeed = withSeed.seed
                }
            }
        }
    }
}

class SeedExceptionWrapper : TestExecutionExceptionHandler {
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

@ExtendWith(SeedExceptionWrapper::class, SeedExtension::class)
abstract class SimulationTestBase {
    var currentSeed: Int = 0
    var explicitSeed: Int? = null
    protected lateinit var rand: Random
    protected lateinit var dispatcher: DeterministicDispatcher

    @BeforeEach
    open fun setUpSimulation() {
        currentSeed = explicitSeed ?: Random.nextInt()
        rand = Random(currentSeed)
        dispatcher = DeterministicDispatcher(currentSeed)
    }

    @AfterEach
    open fun tearDownSimulation() {
        explicitSeed = null
    }

    companion object SimulationIterations {
        /**
         * Returns iteration numbers for simulation tests.
         * Override using:
         *   -Dxtdb.simulation-test-iterations=N
         */
        private val testIterations: Int =
            System.getProperty("xtdb.simulation-test-iterations", "100").toInt()

        @JvmStatic
        fun iterationSource(): List<Arguments> = (1..testIterations).map { Arguments.of(it) }
    }
}