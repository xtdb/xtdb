@file:Suppress("NestedLambdaShadowedImplicitParameter")

package xtdb.corda

import clojure.java.api.Clojure
import clojure.lang.AFunction
import clojure.lang.Keyword
import xtdb.api.*
import xtdb.corda.state.XtdbState
import net.corda.core.crypto.SecureHash
import net.corda.core.node.AppServiceHub

@Suppress("unused")
private val XTDB_CORDA_SERVICE = Clojure.`var`("clojure.core", "require")(Clojure.read("xtdb.corda"))

private val NOTIFY_TX = Clojure.`var`("xtdb.corda/notify-tx")
private val TO_XTDB_TX = Clojure.`var`("xtdb.corda/->xtdb-tx")

@Suppress("unused")
data class XtdbDoc(
    override val xtdbId: Any,
    override val xtdbDoc: Map<String, Any>
) : XtdbState

@Suppress("unused")
class CordaTxLogConfigurator(private val moduleConfigurator: ModuleConfiguration.Builder) {
    // TODO migrate `ModuleConfigurator` to interface
    fun set(key: String, value: Any) { moduleConfigurator.set(key, value) }
    fun set(kvs: Map<String, Any>) { moduleConfigurator.set(kvs) }
    fun with(module: String) { moduleConfigurator.with(module) }
    fun with(module: String, ref: String) { moduleConfigurator.with(module, ref) }
    fun with(module: String, configurator: ModuleConfiguration.Builder.() -> Unit) { moduleConfigurator.with(module) { configurator(it) } }

    fun withDocumentMapping(f: (Any) -> Iterable<XtdbState>?) {
        moduleConfigurator.with("document-mapper") {
            it.set("xtdb/module", object : AFunction() {
                override fun invoke(opts: Any) = object : AFunction() {
                    override fun invoke(cordaState: Any) = f(cordaState)
                }
            })
        }
    }
}

fun NodeConfiguration.Builder.withCordaTxLog(txLogConfigurator: CordaTxLogConfigurator.() -> Unit = {}) {
    with("xtdb/tx-log") {
        it.module("xtdb.corda/->tx-log")
        txLogConfigurator(CordaTxLogConfigurator(it))
    }
}

@Suppress("unused")
fun AppServiceHub.startXtdbNode(configurator: NodeConfiguration.Builder.() -> Unit = {}): IXtdb {
    val hub = this
    val node = IXtdb.startNode {
        it.with("xtdb.corda/service-hub") {
            it.set("xtdb/module", object : AFunction() {
                override fun invoke(deps: Any) = hub
            })
        }
        it.withCordaTxLog()
        configurator(it)
    }

    validatedTransactions.updates.subscribe {
        NOTIFY_TX(it.id, node)
    }

    registerUnloadHandler { node.close() }

    return node
}

@Suppress("UNCHECKED_CAST", "UNUSED")
fun AppServiceHub.xtdbTx(xtdb: IXtdb, id: SecureHash): TransactionInstant? =
    database.transaction {
        TransactionInstant.factory(TO_XTDB_TX(id, xtdb) as Map<Keyword, Any>?)
    }
