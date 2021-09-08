package com.example.service

import xtdb.corda.xtdbTx
import xtdb.corda.startXtdbNode
import xtdb.corda.state.XtdbState
import xtdb.corda.withCordaTxLog
import net.corda.core.crypto.SecureHash
import net.corda.core.node.AppServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.serialization.SingletonSerializeAsToken

@CordaService
class XtdbService(private val serviceHub: AppServiceHub) : SingletonSerializeAsToken() {

    val node = serviceHub.startXtdbNode {
        withCordaTxLog {
            withDocumentMapping {
                if (it is XtdbState) listOf(it)
                else null
            }
        }
    }

    // expose the `xtdbTx` function
    fun xtdbTx(id: SecureHash) = serviceHub.xtdbTx(node, id)
}
