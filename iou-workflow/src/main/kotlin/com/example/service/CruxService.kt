package com.example.service

import crux.corda.cruxTx
import crux.corda.startCruxNode
import net.corda.core.crypto.SecureHash
import net.corda.core.node.AppServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.serialization.SingletonSerializeAsToken

@CordaService
class CruxService(private val serviceHub: AppServiceHub) : SingletonSerializeAsToken() {

    val node = serviceHub.startCruxNode {
        // this :: crux.api.NodeConfigurator
    }

    // expose the `cruxTx` fun
    fun cruxTx(id: SecureHash) = serviceHub.cruxTx(node, id)

}