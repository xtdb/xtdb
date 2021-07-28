package com.example.contract

import crux.corda.state.CruxState
import net.corda.core.contracts.BelongsToContract
import net.corda.core.contracts.ContractState
import net.corda.core.contracts.LinearState
import net.corda.core.contracts.UniqueIdentifier
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.Party

/**
 * The state object recording IOU agreements between two parties.
 *
 * A state must implement [ContractState] or one of its descendants.
 *
 * @param value the value of the IOU.
 * @param lender the party issuing the IOU.
 * @param borrower the party receiving and approving the IOU.
 */
@BelongsToContract(IOUContract::class)
data class IOUState(val value: Int,
                    val lender: Party,
                    val borrower: Party,
                    override val linearId: UniqueIdentifier = UniqueIdentifier()) :
    LinearState, CruxState {

    override val cruxId = linearId.id
    override val cruxDoc: Map<String, Any> = mapOf(
        "iou-state/value" to value,
        "iou-state/lender" to lender.name.toString(),
        "iou-state/borrower" to borrower.name.toString())

    /** The public keys of the involved parties. */
    override val participants: List<AbstractParty> get() = listOf(lender, borrower)
}
