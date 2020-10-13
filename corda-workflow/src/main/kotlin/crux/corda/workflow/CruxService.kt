package crux.corda.workflow

import clojure.java.api.Clojure
import clojure.lang.Keyword
import crux.api.ICruxAPI
import net.corda.core.crypto.SecureHash
import net.corda.core.node.AppServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.serialization.SingletonSerializeAsToken

@CordaService
class CruxService(private val serviceHub: AppServiceHub) : SingletonSerializeAsToken() {
    val cruxNode: ICruxAPI

    @Suppress("UNCHECKED_CAST")
    fun cruxTx(id: SecureHash): Map<Keyword, Any>? =
        serviceHub.database.transaction {
            toCruxTx(id, cruxNode) as Map<Keyword, Any>?
        }

    companion object {
        init {
            Clojure.`var`("clojure.core", "require")(Clojure.read("crux.corda.service"))
        }

        private val startCruxNode = Clojure.`var`("crux.corda.service/start-node")
        private val syncTxs = Clojure.`var`("crux.corda.service/sync-txs")
        private val toCruxTx = Clojure.`var`("crux.corda.service/->crux-tx")
    }

    init {
        cruxNode = startCruxNode(serviceHub) as ICruxAPI
        syncTxs(cruxNode)

        try {
            serviceHub.validatedTransactions.updates.subscribe {
                syncTxs(cruxNode)
            }

            serviceHub.registerUnloadHandler {
                cruxNode.close()
            }
        } catch (e: Exception) {
            cruxNode.close()
        }
    }
}
