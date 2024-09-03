package xtdb.api

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import xtdb.api.tx.TxOps.sql
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
            node.submitTx(sql("INSERT INTO foo (_id) VALUES ('jms')"))
        }

        assertEquals(
            listOf(mapOf("id" to "jms")),

            node.openQuery("SELECT _id AS id FROM foo").toList()
        )

        File("/tmp/test-storage").deleteRecursively()
        File("/tmp/test-log").deleteRecursively()
    }
}
