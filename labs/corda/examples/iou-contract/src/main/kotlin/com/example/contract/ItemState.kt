package com.example.contract

import crux.corda.state.CruxState
import net.corda.core.contracts.BelongsToContract
import net.corda.core.contracts.UniqueIdentifier
import net.corda.core.identity.Party
import net.corda.core.contracts.LinearState
import net.corda.core.identity.AbstractParty

@BelongsToContract(ItemContract::class)
data class ItemState(val name: String,
                     val value: Int,
                     val owner: Party,
                     override val linearId: UniqueIdentifier = UniqueIdentifier()) :
    LinearState, CruxState {

    override val cruxId = linearId.id
    override val cruxDoc: Map<String,Any> = mapOf(
            "item/value" to value,
            "item/name" to name,
            "item/owner" to owner.name.toString())

    override val participants: List<AbstractParty> get() = listOf(owner)
}