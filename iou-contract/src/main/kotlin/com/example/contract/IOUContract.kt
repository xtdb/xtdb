package com.example.contract

import net.corda.core.contracts.CommandData
import net.corda.core.contracts.Contract
import net.corda.core.contracts.requireSingleCommand
import net.corda.core.contracts.requireThat
import net.corda.core.transactions.LedgerTransaction
import java.security.PublicKey

/**
 * A implementation of a basic smart contract in Corda.
 *
 * This contract enforces rules regarding the creation of a valid [IOUState], which in turn encapsulates an [IOUState].
 *
 * For a new [IOUState] to be issued onto the ledger, a transaction is required which takes:
 * - Zero input states.
 * - One output state: the new [IOUState].
 * - A CreateIOU() command with the public keys of both the lender and the borrower.
 *
 * To update an [IOUState] onto the ledger, a transaction is required which takes:
 * - One input states: the old [IOUState].
 * - One output state: the new [IOUState] with the same properties as the old one, except the value.
 * - A UpdateIOU() command with the public keys of both the lender and the borrower.
 *
 * All contracts must sub-class the [Contract] interface.
 */
class IOUContract : Contract {
    companion object {
        @JvmStatic
        val ID = "com.example.contract.IOUContract"
    }

    fun verifyCreate(tx: LedgerTransaction, signers: Set<PublicKey>) {
        requireThat {
            // Generic constraints around the IOU transaction.
            "No inputs should be consumed when issuing an IOU." using (tx.inputs.isEmpty())
            "Only one output state should be created." using (tx.outputs.size == 1)
            val output = tx.outputsOfType<IOUState>().single()
            "The lender and the borrower cannot be the same entity." using (output.lender != output.borrower)
            "All of the participants must be signers." using (signers.containsAll(output.participants.map { it.owningKey }))

            // IOU-specific constraints.
            "The IOU's value must be non-negative." using (output.value > 0)
        }
    }

    fun verifyUpdate(tx: LedgerTransaction, signers: Set<PublicKey>) {
        requireThat {
            "Only one input state should be consumed" using (tx.inputs.size == 1)
            "Only one output state should be created." using (tx.outputs.size == 1)
            val output = tx.outputsOfType<IOUState>().single()
            val input = tx.inputsOfType<IOUState>().single()
            "Input and output state must share ID" using (output.linearId == input.linearId)
            "The lender and the borrower cannot be the same entity." using (output.lender != output.borrower)
            "Lender and borrower are still the same parties, though maybe not the same roles" using
                    (setOf(output.borrower,output.lender) == setOf(input.borrower, input.lender))
            "All of the participants must be signers." using (signers.containsAll(output.participants.map { it.owningKey }))

            // IOU-specific constraints.
            "The IOU's value must be non-negative." using (output.value > 0)
        }
    }

    fun verifyDelete(tx: LedgerTransaction, signers: Set<PublicKey>) {
        requireThat {
            // Generic constraints around the IOU transaction.
            "No outputs should be generated when deleting an IOU." using (tx.outputs.isEmpty())
            "Only one output state should be created." using (tx.inputs.size == 1)
            val input = tx.inputsOfType<IOUState>().single()
            "The lender and the borrower cannot be the same entity." using (input.lender != input.borrower)
            "All of the participants must be signers." using (signers.containsAll(input.participants.map { it.owningKey }))

            // IOU-specific constraints.
            "The IOU's value must be non-negative." using (input.value > 0)
        }
    }

    override fun verify(tx: LedgerTransaction) {
        val command = tx.commands.requireSingleCommand<Commands>()
        val signers = command.signers.toSet()
        when (command.value) {
            is Commands.CreateIOU -> verifyCreate(tx, signers)
            is Commands.UpdateIOU -> verifyUpdate(tx, signers)

        }
    }

    interface Commands : CommandData {
        class CreateIOU : Commands
        class UpdateIOU : Commands
        class DeleteIOU : Commands
    }
}
