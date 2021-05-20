package utils

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail

class FailingTestTest {
    @Test
    fun `this will fail I want to see how it appears in github`() {
        fail("Fs in chat")
    }
}