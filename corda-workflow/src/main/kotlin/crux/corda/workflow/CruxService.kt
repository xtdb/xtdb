package crux.corda.workflow

import clojure.java.api.Clojure
import crux.api.ICruxAPI
import net.corda.core.node.AppServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.serialization.SingletonSerializeAsToken

@CordaService
class CruxService(private val serviceHub: AppServiceHub) : SingletonSerializeAsToken() {

    private val cruxNode: ICruxAPI

    companion object {
        init {
            Clojure.`var`("clojure.core", "require")(Clojure.read("crux.corda.service"))
        }

        private val startCruxNode = Clojure.`var`("crux.corda.service/start-node")
        private val processTx = Clojure.`var`("crux.corda.service/process-tx")
    }

    private fun setupCruxTxsTable() {
        serviceHub.jdbcSession().createStatement().use { stmt ->
            stmt.execute("""
                CREATE TRIGGER IF NOT EXISTS crux_tx_trigger
                  AFTER INSERT, UPDATE ON node_transactions
                  FOR EACH ROW 
                  CALL "crux.corda.workflow.NodeTransactionTrigger"""".trimIndent())

            stmt.execute("""
                CREATE TABLE IF NOT EXISTS crux_txs (
                  crux_tx_id IDENTITY NOT NULL PRIMARY KEY,
                  crux_tx_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                  corda_tx_id VARCHAR(64) NOT NULL UNIQUE REFERENCES node_transactions(tx_id)
                )""".trimIndent())
        }
    }

    init {
        cruxNode = startCruxNode(serviceHub) as ICruxAPI

        setupCruxTxsTable()

        try {
            serviceHub.validatedTransactions.updates.subscribe { tx ->
                processTx(cruxNode, serviceHub, tx)
            }

            serviceHub.registerUnloadHandler {
                cruxNode.close()
            }
        } catch (e: Exception) {
            cruxNode.close()
        }
    }
}