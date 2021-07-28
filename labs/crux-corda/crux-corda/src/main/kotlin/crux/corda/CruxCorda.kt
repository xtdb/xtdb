@file:Suppress("NestedLambdaShadowedImplicitParameter")

package crux.corda

import clojure.java.api.Clojure
import clojure.lang.AFunction
import clojure.lang.Keyword
import crux.api.*
import crux.corda.state.CruxState
import net.corda.core.crypto.SecureHash
import net.corda.core.node.AppServiceHub

@Suppress("unused")
private val CRUX_CORDA_SERVICE = Clojure.`var`("clojure.core", "require")(Clojure.read("crux.corda"))

private val SYNC_TXS = Clojure.`var`("crux.corda/sync-txs")
private val TO_CRUX_TX = Clojure.`var`("crux.corda/->crux-tx")

@Suppress("unused")
data class CruxDoc(
    override val cruxId: Any,
    override val cruxDoc: Map<String, Any>
) : CruxState

@Suppress("unused")
class CordaTxLogConfigurator(private val moduleConfigurator: ModuleConfiguration.Builder) {
    // TODO migrate `ModuleConfigurator` to interface
    fun set(key: String, value: Any) { moduleConfigurator.set(key, value) }
    fun set(kvs: Map<String, Any>) { moduleConfigurator.set(kvs) }
    fun with(module: String) { moduleConfigurator.with(module) }
    fun with(module: String, ref: String) { moduleConfigurator.with(module, ref) }
    fun with(module: String, configurator: ModuleConfiguration.Builder.() -> Unit) { moduleConfigurator.with(module) { configurator(it) } }

    fun withDocumentMapping(f: (Any) -> Iterable<CruxState>?) {
        moduleConfigurator.with("document-mapper") {
            it.set("crux/module", object : AFunction() {
                override fun invoke(opts: Any) = object : AFunction() {
                    override fun invoke(cordaState: Any) = f(cordaState)
                }
            })
        }
    }
}

fun NodeConfiguration.Builder.withCordaTxLog(txLogConfigurator: CordaTxLogConfigurator.() -> Unit = {}) {
    with("crux/tx-log") {
        it.module("crux.corda/->tx-log")
        txLogConfigurator(CordaTxLogConfigurator(it))
    }
}

@Suppress("unused")
fun AppServiceHub.startCruxNode(configurator: NodeConfiguration.Builder.() -> Unit = {}): ICruxAPI {
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
fun AppServiceHub.cruxTx(cruxNode: ICruxAPI, id: SecureHash): TransactionInstant? =
    database.transaction {
        TransactionInstant.factory(TO_CRUX_TX(id, cruxNode) as Map<Keyword, Any>?)
    }
