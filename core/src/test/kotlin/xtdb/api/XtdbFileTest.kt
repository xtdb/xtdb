package xtdb.api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import xtdb.api.query.XtqlQuery
import xtdb.api.tx.TxOp
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
        node.submitTx(TxOp.put("foo", mapOf("xt/id" to "jms")))

        assertEquals(
            listOf(mapOf("id" to "jms")),

            node.openQuery(
                XtqlQuery.from("foo") { "xt/id" boundTo "id" }
            ).toList()
        )
    }

    @Test
    fun validConfigSubmitClient() {
        val resourcePath = XtdbFileTest::class.java.classLoader.getResource("submit-client-config.yaml")!!.path
        val submitClient = assertDoesNotThrow { XtdbSubmitClient.openSubmitClient(path = Path(resourcePath)) }

        assertDoesNotThrow {
            submitClient.submitTx(TxOp.put("foo", mapOf("xt/id" to "jms")))
        }

    }
}