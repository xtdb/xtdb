package crux.corda.workflow

import clojure.java.api.Clojure
import crux.api.ICruxAPI
import net.corda.core.node.AppServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.serialization.SingletonSerializeAsToken

@CordaService
class CruxService(serviceHub: AppServiceHub) : SingletonSerializeAsToken() {

    val cruxNode: ICruxAPI

    companion object {
        init {
            Clojure.`var`("clojure.core", "require")(Clojure.read("crux.corda.service"))
        }

        private val startCruxNode = Clojure.`var`("crux.corda.service/start-node")
        private val processTx = Clojure.`var`("crux.corda.service/process-tx")
    }

    init {
        cruxNode = startCruxNode(serviceHub) as ICruxAPI

        try {
            serviceHub.validatedTransactions.updates.subscribe { tx ->
                processTx(cruxNode, tx)
            }

            serviceHub.registerUnloadHandler {
                cruxNode.close()
            }
        } catch (e: Exception) {
            cruxNode.close()
        }
    }
}
