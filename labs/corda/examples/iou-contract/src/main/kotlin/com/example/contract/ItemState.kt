package com.example.contract

import xtdb.corda.state.XtdbState
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
    LinearState, XtdbState {

    override val xtdbId = linearId.id
    override val xtdbDoc: Map<String,Any> = mapOf(
            "item/value" to value,
            "item/name" to name,
            "item/owner" to owner.name.toString())

    override val participants: List<AbstractParty> get() = listOf(owner)
}
