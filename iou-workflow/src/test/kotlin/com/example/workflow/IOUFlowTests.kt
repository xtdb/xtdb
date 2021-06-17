package com.example.workflow

import com.example.contract.IOUState
import com.example.service.CruxService
import net.corda.core.node.services.queryBy
import net.corda.core.utilities.getOrThrow
import net.corda.testing.core.singleIdentity
import net.corda.testing.node.MockNetwork
import net.corda.testing.node.MockNetworkParameters
import net.corda.testing.node.StartedMockNode
import net.corda.testing.node.TestCordapp
import org.junit.After
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals

class IOUFlowTests {
    private lateinit var network: MockNetwork
    private lateinit var a: StartedMockNode
    private lateinit var b: StartedMockNode

    @Before
    fun setup() {
        network = MockNetwork(MockNetworkParameters(cordappsForAllNodes = listOf(
            TestCordapp.findCordapp("com.example.contract"),
            TestCordapp.findCordapp("com.example.workflow"),
            TestCordapp.findCordapp("com.example.service"))))
        a = network.createPartyNode()
        b = network.createPartyNode()
        // For real nodes this happens automatically, but we have to manually register the flow for tests.
        listOf(a, b).forEach { it.registerInitiatedFlow(ExampleFlow.Acceptor::class.java) }
        network.runNetwork()
    }

    @After
    fun tearDown() {
        network.stopNodes()
    }

    @Test
    fun `flow records the correct IOU in both parties' vaults`() {
        val nodes = listOf(a, b)

        val iouValue = 1
        val flow = ExampleFlow.Initiator(1, b.info.singleIdentity())
        val future = a.startFlow(flow)
        network.runNetwork()

        val signedTx = future.getOrThrow()

        // We check the recorded transaction in both transaction storages.
        for (node in nodes) {
            assertEquals(signedTx, node.services.validatedTransactions.getTransaction(signedTx.id))
        }

        // We check the recorded IOU in both vaults.
        for (node in nodes) {
            node.transaction {
                val ious = node.services.vaultService.queryBy<IOUState>().states
                assertEquals(1, ious.size)
                val recordedState = ious.single().state.data
                assertEquals(recordedState.value, iouValue)
                assertEquals(recordedState.lender, a.info.singleIdentity())
                assertEquals(recordedState.borrower, b.info.singleIdentity())
            }
        }

        // We check Crux gets a transaction
        for (node in nodes) {
            val cruxService = node.services.cordaService(CruxService::class.java)
            val cruxNode = cruxService.node

            assertEquals(1L, cruxService.cruxTx(signedTx.id)!!.id)
            assertEquals(1L, cruxNode.latestCompletedTx().id)

            assertEquals(
                listOf(a.info.singleIdentity().name.toString(), b.info.singleIdentity().name.toString(), 1L),
                cruxNode.db().query("""
                    {:find [?l ?b ?v] 
                     :where [[?iou :iou-state/lender ?l]
                             [?iou :iou-state/borrower ?b]
                             [?iou :iou-state/value ?v]]}""".trimIndent())
                    .first())
        }
    }
}
