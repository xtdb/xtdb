package com.example.service

import crux.corda.cruxTx
import crux.corda.startCruxNode
import crux.corda.state.CruxState
import crux.corda.withCordaTxLog
import net.corda.core.crypto.SecureHash
import net.corda.core.node.AppServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.serialization.SingletonSerializeAsToken

@CordaService
class CruxService(private val serviceHub: AppServiceHub) : SingletonSerializeAsToken() {

    val node = serviceHub.startCruxNode {
        withCordaTxLog {
            withDocumentMapping {
                if (it is CruxState) listOf(it)
                else null
            }
        }
    }

    // expose the `cruxTx` function
    fun cruxTx(id: SecureHash) = serviceHub.cruxTx(node, id)
}