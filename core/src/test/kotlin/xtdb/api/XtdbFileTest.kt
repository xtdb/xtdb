package xtdb.api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import xtdb.api.query.XtqlQuery.Queries.from
import xtdb.api.tx.putDocs
import java.io.File
import kotlin.io.path.Path

internal class XtdbFileTest {

    @Test
    fun nonYamlConfigFile() {
        val thrown = assertThrows<IllegalArgumentException> {
            Xtdb.openNode(path = Path("config-file.edn"))
        }

        assertEquals( "Invalid config file type - must be '.yaml'", thrown.message)
    }

    @Test
    fun nonExistentConfigFile() {
        val thrown = assertThrows<IllegalArgumentException> {
            Xtdb.openNode(path = Path("non-existent-file.yaml"))
        }

        assertEquals( "Provided config file does not exist", thrown.message)
    }

    @Test
    fun validConfigFile() {
        val resourcePath = XtdbFileTest::class.java.classLoader.getResource("node-config.yaml")!!.path
        val node = assertDoesNotThrow { Xtdb.openNode(path = Path(resourcePath)) }

        // can use the created node
        assertDoesNotThrow {
            node.submitTx(putDocs("foo", mapOf("xt\$id" to "jms")))
        }

        assertEquals(
            listOf(mapOf("id" to "jms")),

            node.openQuery(
                from("foo") { "xt\$id" boundTo "id" }
            ).toList()
        )

        File("/tmp/test-storage").deleteRecursively()
        File("/tmp/test-log").deleteRecursively()
    }

    @Test
    fun validConfigSubmitClient() {
        val resourcePath = XtdbFileTest::class.java.classLoader.getResource("submit-client-config.yaml")!!.path
        val submitClient = assertDoesNotThrow { XtdbSubmitClient.openSubmitClient(path = Path(resourcePath)) }

        assertDoesNotThrow {
            submitClient.submitTx(putDocs("foo", mapOf("xt\$id" to "jms")))
        }

        File("/tmp/test-log").deleteRecursively()
    }
}
