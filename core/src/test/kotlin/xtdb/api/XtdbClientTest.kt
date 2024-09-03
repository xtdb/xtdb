package xtdb.api

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import xtdb.api.tx.TxOps.sql
import java.net.URI

class XtdbClientTest {

    private lateinit var node: IXtdb
    private lateinit var client: IXtdb

    @BeforeEach
    fun setUp() {
        node = Xtdb.openNode(Xtdb.Config().module(HttpServer.Factory(3000)))
        client = XtdbClient.openClient(URI("http://localhost:3000").toURL())
    }

    @AfterEach
    fun tearDown() {
        client.close()
        node.close()
    }

    @Test
    fun `test executeTx returning transaction result 3491`() {
        val res = client.executeTx(sql("INSERT INTO foo (_id) VALUES ('jms')"))
        assertInstanceOf(TransactionResult::class.java , res)
    }
}