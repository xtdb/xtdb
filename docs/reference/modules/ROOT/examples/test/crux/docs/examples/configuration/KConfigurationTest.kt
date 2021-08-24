package crux.docs.examples.configuration

import org.junit.*
import org.junit.Assert.*
import java.io.IOException
import java.io.File
import java.net.URL
import java.net.URISyntaxException

// tag::import[]
import crux.api.Crux
// end::import[]

class KConfigurationTest {
    public fun `Starting a Crux node from file`() {
        // tag::from-file[]
        val cruxNode: ICruxAPI = Crux.startNode(File("config.json"))
        // end::from-file[]

        close(cruxNode)
    }

    public fun `Starting a Crux node from resource`() {
        // tag::from-resource[]
        val cruxNode = Crux.startNode(MyApp::class.java.getResource("config.json"))
        // end::from-resource[]

        close(cruxNode)
    }

    public fun `Starting a Crux node with configurator`() {
        // tag::from-configurator[]
        val cruxNode = Crux.startNode { n ->
            // ...
        }
        // end::from-configurator[]
    }

    public fun `Starting a Crux node with http server`() {
        // tag::http-server[]
        val cruxNode = Crux.startNode { n ->
            n.with("crux.http-server/server") { http ->
                http["port"] = 3000
            }
        }
        // end::http-server[]

        close(cruxNode)
    }

    public fun `Starting a Crux node and overriding a module implementation`() {
        // tag::override-module[]
        val cruxNode = Crux.startNode { n ->
            n.with("xt/document-store") { docStore ->
                docStore.module("crux.s3/->document-store")
                docStore["bucket"] = "my-bucket"
                docStore["prefix"] = "my-prefix"
            }
        }
        // end::override-module[]

        close(cruxNode)
    }

    public fun `Starting a Crux node with nested modules`() {
        // tag::nested-modules-0[]
        val cruxNode = Crux.startNode{ n ->
            n.with("xt/tx-log") { txLog ->
                txLog.with("kv-store") { kv ->
                    kv.module("crux.rocksdb/->kv-store")
                    kv["db-dir"] = File("/tmp/rocksdb")
                }
            }
            // end::nested-modules-0[]
            /*
            This obviously won't compile so has to be commented out
            // tag::nested-modules-1[]
            n.with("xt/document-store") { docStore -> ... }
            n.with("xt/index-store") { indexStore -> ... }
            // end::nested-modules-1[]
             */
            // tag::nested-modules-2[]
        }
        // end::nested-modules-2[]

        close(cruxNode)
    }

    public fun `Starting a Crux node with shared modules`() {
        // tag::sharing-modules[]
        val cruxNode = Crux.startNode { n ->
            n.with("my-rocksdb") { rocks ->
                rocks.module("crux.rocksdb/->kv-store")
                rocks["db-dir"] = File("/tmp/rocksdb")
            }
            n.with("xt/document-store") { docStore ->
                docStore["kv-store"] = "my-rocksdb"
            }
            n.with("xt/tx-log") { txLog ->
                txLog["kv-store"] = "my-rocksdb"
            }
        }
        // end::sharing-modules[]

        close(cruxNode)
    }

    private fun close(cruxNode: ICruxAPI) {
        try {
            cruxNode.close()
        } catch (e: IOException) {
            fail()
        }
    }
}
