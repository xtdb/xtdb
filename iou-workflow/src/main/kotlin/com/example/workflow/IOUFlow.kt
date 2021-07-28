package com.example.workflow

import co.paralleluniverse.fibers.Suspendable
import com.example.contract.IOUContract
import com.example.contract.IOUState
import net.corda.core.contracts.Command
import net.corda.core.contracts.requireThat
import net.corda.core.flows.*
import net.corda.core.identity.Party
import net.corda.core.node.services.Vault
import net.corda.core.node.services.queryBy
import net.corda.core.node.services.vault.QueryCriteria
import net.corda.core.transactions.SignedTransaction
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.ProgressTracker
import java.nio.channels.GatheringByteChannel

/**
 * This flow allows two parties (the [Initiator] and the [Acceptor]) to come to an agreement about the IOU encapsulated
 * within an [IOUState].
 *
 * In our simple example, the [Acceptor] always accepts a valid IOU.
 *
 * These flows have deliberately been implemented by using only the call() method for ease of understanding. In
 * practice we would recommend splitting up the various stages of the flow into sub-routines.
 *
 * All methods called within the [FlowLogic] sub-class need to be annotated with the @Suspendable annotation.
 */
object IOUFlow {
    @InitiatingFlow
    @StartableByRPC
    class Initiator(val iouValue: Int,
                    val otherParty: Party) : FlowLogic<SignedTransaction>() {
        /**
         * The progress tracker checkpoints each stage of the flow and outputs the specified messages when each
         * checkpoint is reached in the code. See the 'progressTracker.currentStep' expressions within the call() function.
         */
        companion object {
            object GENERATING_TRANSACTION : ProgressTracker.Step("Generating transaction based on new IOU.")
            object VERIFYING_TRANSACTION : ProgressTracker.Step("Verifying contract constraints.")
            object SIGNING_TRANSACTION : ProgressTracker.Step("Signing transaction with our private key.")
            object GATHERING_SIGS : ProgressTracker.Step("Gathering the counterparty's signature.") {
                override fun childProgressTracker() = CollectSignaturesFlow.tracker()
            }

            object FINALISING_TRANSACTION : ProgressTracker.Step("Obtaining notary signature and recording transaction.") {
                override fun childProgressTracker() = FinalityFlow.tracker()
            }

            fun tracker() = ProgressTracker(
                GENERATING_TRANSACTION,
                VERIFYING_TRANSACTION,
                SIGNING_TRANSACTION,
                GATHERING_SIGS,
                FINALISING_TRANSACTION
            )
        }

        override val progressTracker = tracker()

        lateinit var txBuilder: TransactionBuilder
        lateinit var txCommand: Command<IOUContract.Commands>
        lateinit var iouState: IOUState
        /**
         * The flow logic is encapsulated within the call() method.
         */
        @Suspendable
        override fun call(): SignedTransaction {

            val notary = serviceHub.networkMapCache.notaryIdentities.single()
            val myId = serviceHub.myInfo.legalIdentities.first()

            /**
             * Find any unconsumed states where both parties (ourselves and the other participant) are involved
             */
            val oldIOUList = serviceHub.vaultService.queryBy<IOUState>(QueryCriteria.VaultQueryCriteria(Vault.StateStatus.UNCONSUMED)).states.filter {
                setOf(myId, otherParty) == setOf(it.state.data.borrower, it.state.data.lender)
            }

            // Stage 1.
            progressTracker.currentStep = GENERATING_TRANSACTION

            /**
             * Build a new IOUState and Command
             */
            when {
                // 1. brand new IOUState -> CreateIOU Command
                oldIOUList.isEmpty() -> {
                    iouState = IOUState(value = iouValue,
                            lender = myId,
                            borrower = otherParty)
                    txCommand = Command(IOUContract.Commands.CreateIOU(), iouState.participants.map { it.owningKey })
                    txBuilder = TransactionBuilder(notary)
                            .addOutputState(iouState, IOUContract.ID)
                            .addCommand(txCommand)
                }
                // 2. Existing UNCONSUMED IOUState -> UpdateIOU Command
                oldIOUList.size == 1 -> {
                    val oldIOUData = oldIOUList.single().state.data
                    // 2.1. We lend more money, new IOUState value is the sum of old and new value
                    if (oldIOUData.lender == myId) {
                        iouState = oldIOUData.copy(value = iouValue + oldIOUData.value)
                    // 2.2. Money is borrowed from us
                    } else {
                        // 2.2.1 More money is borrowed from us than we borrowed previously
                        // borrower and lender switch places
                        if (iouValue > oldIOUData.value) {
                            iouState = oldIOUData.copy(
                                    value = iouValue - oldIOUData.value,
                                    borrower = oldIOUData.lender,
                                    lender = oldIOUData.borrower)
                        // 2.2.2 Less money is borrowed from us than we borrowed previously
                        } else {
                            iouState = oldIOUData.copy(value = oldIOUData.value - iouValue)
                        }

                    }
                    txCommand = Command(IOUContract.Commands.UpdateIOU(), iouState.participants.map { it.owningKey })
                    txBuilder = TransactionBuilder(notary)
                            .addInputState(oldIOUList.single())
                            .addOutputState(iouState, IOUContract.ID)
                            .addCommand(txCommand)
                }
                // 3. Lending and borrowing between two parties is a single linear state
                else -> {
                    throw FlowException("more than one UNCONSUMED state where the borrower and lender are the same")
                }
            }

            // Stage 2.
            progressTracker.currentStep = VERIFYING_TRANSACTION
            // Verify that the transaction is valid.
            txBuilder.verify(serviceHub)

            // Stage 3.
            progressTracker.currentStep = SIGNING_TRANSACTION
            // Sign the transaction.
            val partSignedTx = serviceHub.signInitialTransaction(txBuilder)

            // Stage 4.
            progressTracker.currentStep = GATHERING_SIGS
            // Send the state to the counterparty, and receive it back with their signature.
            val otherPartySession = initiateFlow(otherParty)
            val fullySignedTx = subFlow(CollectSignaturesFlow(partSignedTx, setOf(otherPartySession), GATHERING_SIGS.childProgressTracker()))

            // Stage 5.
            progressTracker.currentStep = FINALISING_TRANSACTION
            // Notarise and record the transaction in both parties' vaults.
            return subFlow(FinalityFlow(fullySignedTx, setOf(otherPartySession), FINALISING_TRANSACTION.childProgressTracker()))
        }
    }

    @InitiatedBy(Initiator::class)
    class Acceptor(val otherPartySession: FlowSession) : FlowLogic<SignedTransaction>() {
        @Suspendable
        override fun call(): SignedTransaction {
            val signTransactionFlow = object : SignTransactionFlow(otherPartySession) {
                override fun checkTransaction(stx: SignedTransaction) = requireThat {
                    val output = stx.tx.outputs.single().data
                    "This must be an IOU transaction." using (output is IOUState)
                    val iou = output as IOUState
                    "I won't accept IOUs with a value over 100." using (iou.value <= 100)
                }
            }
            val txId = subFlow(signTransactionFlow).id

            return subFlow(ReceiveFinalityFlow(otherPartySession, expectedTxId = txId))
        }
    }
}

object DeleteIOUFlow {
    @InitiatingFlow
    @StartableByRPC
    class Initiator(val otherParty: Party) : FlowLogic<SignedTransaction>() {
        companion object {
            object GENERATING_TRANSACTION : ProgressTracker.Step("Generating transaction based on new IOU.")
            object VERIFYING_TRANSACTION : ProgressTracker.Step("Verifying contract constraints.")
            object SIGNING_TRANSACTION : ProgressTracker.Step("Signing transaction with our private key.")
            object GATHERING_SIGS : ProgressTracker.Step("Gathering the counterparty's signature.") {
                override fun childProgressTracker() = CollectSignaturesFlow.tracker()
            }

            object FINALISING_TRANSACTION : ProgressTracker.Step("Obtaining notary signature and recording transaction.") {
                override fun childProgressTracker() = FinalityFlow.tracker()
            }

            fun tracker() = ProgressTracker(
                    GENERATING_TRANSACTION,
                    VERIFYING_TRANSACTION,
                    SIGNING_TRANSACTION,
                    GATHERING_SIGS,
                    FINALISING_TRANSACTION
            )
        }

        override val progressTracker = DeleteIOUFlow.Initiator.tracker()

        @Suspendable
        override fun call(): SignedTransaction {

            val notary = serviceHub.networkMapCache.notaryIdentities.single()
            val myId = serviceHub.myInfo.legalIdentities.first()

            progressTracker.currentStep = GENERATING_TRANSACTION

            /**
             * Find any unconsumed states where [otherParty] owes us money
             */
            val oldIOUList = serviceHub.vaultService.queryBy<IOUState>(QueryCriteria.VaultQueryCriteria(Vault.StateStatus.UNCONSUMED)).states.filter {
                (it.state.data.borrower == otherParty) and (it.state.data.lender == myId)
            }

            when {
                oldIOUList.isEmpty() -> throw FlowException("No active state was found where the initiator is owed money by the other party")
                oldIOUList.size > 1 -> throw FlowException("More than one IOUState with the same lender and borrower was found. This should not be possible")
            }

            val txCommand = Command(IOUContract.Commands.DeleteIOU(), listOf(myId.owningKey, otherParty.owningKey))
            val txBuilder = TransactionBuilder(notary)
                    .addInputState(oldIOUList.single())
                    .addCommand(txCommand)

            progressTracker.currentStep = VERIFYING_TRANSACTION
            txBuilder.verify(serviceHub)

            progressTracker.currentStep = SIGNING_TRANSACTION
            val partSignedTx = serviceHub.signInitialTransaction(txBuilder)


            progressTracker.currentStep = GATHERING_SIGS
            val otherPartySession = initiateFlow(otherParty)
            val fullySignedTx = subFlow(CollectSignaturesFlow(partSignedTx, setOf(otherPartySession), GATHERING_SIGS.childProgressTracker()))

            progressTracker.currentStep = FINALISING_TRANSACTION
            return subFlow(FinalityFlow(fullySignedTx, setOf(otherPartySession), FINALISING_TRANSACTION.childProgressTracker()))
        }
    }

    @InitiatedBy(Initiator::class)
    class Acceptor(val otherPartySession: FlowSession) : FlowLogic<SignedTransaction>() {
        @Suspendable
        override fun call(): SignedTransaction {
            val signTransactionFlow = object : SignTransactionFlow(otherPartySession) {
                override fun checkTransaction(stx: SignedTransaction) = requireThat {
                    val input = stx.tx.inputs
                    "This must be a single input state transaction." using (input.size == 1)
                    val oldState = serviceHub.toStateAndRef<IOUState>(input.single())
                    "I must be the borrower, not the lender" using (oldState.state.data.borrower == serviceHub.myInfo.legalIdentities.first())
                }
            }
            val txId = subFlow(signTransactionFlow).id

            return subFlow(ReceiveFinalityFlow(otherPartySession, expectedTxId = txId))
        }

    }
}