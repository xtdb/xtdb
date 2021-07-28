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
import org.hibernate.sql.Delete
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.*
import kotlin.test.assertEquals

@Suppress("DANGEROUS_CHARACTERS")
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
        listOf(a, b).forEach { it.registerInitiatedFlow(IOUFlow.Acceptor::class.java) }
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
        val flow = IOUFlow.Initiator(1, b.info.singleIdentity())
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
            val inThreeDays = Date(LocalDateTime.now().plusDays(3).toEpochSecond(ZoneOffset.UTC)*1000)
            val threeDaysAgo = Date(LocalDateTime.now().minusDays(3).toEpochSecond(ZoneOffset.UTC)*1000)

            assertEquals(1L, cruxService.cruxTx(signedTx.id)!!.id)
            assertEquals(1L, cruxNode.latestCompletedTx().id)

            // Crux knows about the transaction now
            assertEquals(
                listOf(a.info.singleIdentity().name.toString(), b.info.singleIdentity().name.toString(), 1L),
                cruxNode.db().query("""
                    {:find [?l ?b ?v] 
                     :where [[?iou :iou-state/lender ?l]
                             [?iou :iou-state/borrower ?b]
                             [?iou :iou-state/value ?v]]}""".trimIndent())
                    .first()
            )
            // Crux knows about the transaction three days from now
            assertEquals(
                listOf(a.info.singleIdentity().name.toString(), b.info.singleIdentity().name.toString(), 1L),
                cruxNode.db(inThreeDays).query("""
                    {:find [?l ?b ?v] 
                     :where [[?iou :iou-state/lender ?l]
                             [?iou :iou-state/borrower ?b]
                             [?iou :iou-state/value ?v]]}""".trimIndent()).first()
            )
            // Crux does not know about the transaction three days ago
            assertEquals(
                emptySet(),
                cruxNode.db(threeDaysAgo).query("""
                    {:find [?l ?b ?v] 
                     :where [[?iou :iou-state/lender ?l]
                             [?iou :iou-state/borrower ?b]
                             [?iou :iou-state/value ?v]]}"""
                        .trimIndent())
            )
        }
    }

    @Test
    fun `A lends money to B, B buys a "house"`() {
        // A lends 23 to B
        val iouValue = 23
        val iouFlow = IOUFlow.Initiator(iouValue, b.info.singleIdentity())
        val future = a.startFlow(iouFlow)
        network.runNetwork()
        future.getOrThrow()

        // We verify B has received the funds
        val cruxService = b.services.cordaService(CruxService::class.java)
        val currentDb = cruxService.node.db()
        val bName = b.info.singleIdentity().name.toString()

        assertEquals(
                23L,
                currentDb.query("""
                    {:find [(sum ?v)] 
                     :in [?b]
                     :where [[?iou :iou-state/borrower ?b]
                             [?iou :iou-state/value ?v]]}
            """.trimIndent(), bName).single().single())

        // B buys a house. See ItemFlow
        val itemName = "house"
        val itemValue = 3
        val itemFlow = ItemFlow.Initiator(itemValue, itemName)
        b.startFlow(itemFlow)
        network.runNetwork()

        // We get a new instance of the DB and check
        // which items have been bought with money borrowed from A
        val newDb = cruxService.node.db()
        assertEquals(
                listOf("house", 3L),
                newDb.query("""
                    {:find [?name ?value] 
                     :in [?lender]
                     :where [[?iou :iou-state/borrower ?borrower]
                             [?iou :iou-state/lender ?lender]
                             [?item :item/owner ?borrower]
                             [?item :item/name ?name]
                             [?item :item/value ?value]]}
            """.trimIndent(), a.info.singleIdentity().name.toString()).single())

    }

    @Test
    fun `A lends 10 to B, but then B lends 20 to A, resulting in A owing 10`() {
        // A lends 10 to B
        val aId = a.info.singleIdentity()
        val bId = b.info.singleIdentity()
        val firstIouFlow = IOUFlow.Initiator(10, bId)
        val firstFuture = a.startFlow(firstIouFlow)
        network.runNetwork()
        firstFuture.getOrThrow()

        val firstCheckpoint = Date()
        Thread.sleep(2000)

        // B lends 20 to A
        val secondIouFlow = IOUFlow.Initiator(20, aId)
        val secondFuture = b.startFlow(secondIouFlow)
        network.runNetwork()
        secondFuture.getOrThrow()

        // We get 2 separate Crux instances, one after each transaction
        val firstDB = a.services.cordaService(CruxService::class.java).node.db(firstCheckpoint)
        val secondDB = a.services.cordaService(CruxService::class.java).node.db()

        // After the first transaction, B owes A money
        assertEquals(
                listOf(10L, aId.toString(), bId.toString()),
                firstDB.query("""
                    {:find [?v ?l ?b] 
                     :where [[?iou :iou-state/borrower ?b]
                             [?iou :iou-state/lender ?l]
                             [?iou :iou-state/value ?v]]}
                """.trimIndent()).single())

        // After the second transaction, A owes B money
        assertEquals(
                listOf(10L, bId.toString(), aId.toString()),
                secondDB.query("""
                    {:find [?v ?l ?b] 
                     :where [[?iou :iou-state/borrower ?b]
                             [?iou :iou-state/lender ?l]
                             [?iou :iou-state/value ?v]]}
                """.trimIndent()).single())

        // It is the same CRUX fact too
        assertEquals(
                firstDB.query("""
                    {:find [?id] 
                     :in [?l]
                     :where [[?iou :crux.db/id ?id]
                             [?iou :iou-state/lender ?l]]}
                """.trimIndent(), aId.toString()),
                secondDB.query("""
                    {:find [?id] 
                     :in [?b]
                     :where [[?iou :crux.db/id ?id]
                             [?iou :iou-state/borrower ?b]]}
                """.trimIndent(), aId.toString())

        )
    }

    @Test
    fun `A lends 10 to B, but then retracts and deletes the IOU`() {
        // A lends 10 to B
        val aId = a.info.singleIdentity()
        val bId = b.info.singleIdentity()
        val iouFlow = IOUFlow.Initiator(10, bId)
        val firstFuture = a.startFlow(iouFlow)
        network.runNetwork()
        firstFuture.getOrThrow()

        val firstCheckpoint = Date()
        Thread.sleep(2000)

        // A retracts
        val deleteIouFlow = DeleteIOUFlow.Initiator(bId)
        val secondFuture = a.startFlow(deleteIouFlow)
        network.runNetwork()
        secondFuture.getOrThrow()

        // We get 2 separate Crux instances, one after each transaction
        val firstDB = a.services.cordaService(CruxService::class.java).node.db(firstCheckpoint)
        val secondDB = a.services.cordaService(CruxService::class.java).node.db()

        // After the first transaction, at firstCheckpoint, B owes A money
        assertEquals(
                listOf(10L, aId.toString(), bId.toString()),
                firstDB.query("""
                    {:find [?v ?l ?b] 
                     :where [[?iou :iou-state/borrower ?b]
                             [?iou :iou-state/lender ?l]
                             [?iou :iou-state/value ?v]]}
                """.trimIndent()).single())

        // After the second transaction, no facts are visible
        assertEquals(
                emptySet(),
                secondDB.query("""
                    {:find [?v ?l ?b] 
                     :where [[?iou :iou-state/borrower ?b]
                             [?iou :iou-state/lender ?l]
                             [?iou :iou-state/value ?v]]}
                """.trimIndent()))
    }
}
