@file:Suppress("NestedLambdaShadowedImplicitParameter")

package crux.corda

import clojure.java.api.Clojure
import clojure.lang.AFunction
import clojure.lang.Keyword
import crux.api.Crux
import crux.api.ICruxAPI
import crux.api.ModuleConfigurator
import crux.api.NodeConfigurator
import net.corda.core.crypto.SecureHash
import net.corda.core.node.AppServiceHub

@Suppress("unused")
private val CRUX_CORDA_SERVICE = Clojure.`var`("clojure.core", "require")(Clojure.read("crux.corda"))

private val SYNC_TXS = Clojure.`var`("crux.corda/sync-txs")
private val TO_CRUX_TX = Clojure.`var`("crux.corda/->crux-tx")

fun NodeConfigurator.withCordaTxLog(txLogConfigurator: ModuleConfigurator.() -> Unit = {}) {
    with("crux/tx-log") {
        it.module("crux.corda/->tx-log")
        txLogConfigurator(it)
    }
}

@Suppress("unused")
fun AppServiceHub.startCruxNode(configurator: NodeConfigurator.() -> Unit = {}): ICruxAPI {
    val hub = this
    val node = Crux.startNode {
        it.with("crux.corda/service-hub") {
            it.set("crux/module", object : AFunction() {
                override fun invoke(deps: Any): Any {
                    return hub
                }
            })
        }
        it.withCordaTxLog()
        configurator(it)
    }

    SYNC_TXS(node)

    validatedTransactions.updates.subscribe {
        SYNC_TXS(node)
    }

    registerUnloadHandler { node.close() }

    return node
}

@Suppress("UNCHECKED_CAST", "UNUSED")
fun AppServiceHub.cruxTx(cruxNode: ICruxAPI, id: SecureHash): Map<Keyword, Any>? =
    database.transaction {
        TO_CRUX_TX(id, cruxNode) as Map<Keyword, Any>?
    }
