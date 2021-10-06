package xtdb.docs.examples.configuration

import org.junit.*
import org.junit.Assert.*
import java.io.IOException
import java.io.File

// tag::import[]
import xtdb.api.IXtdb

// end::import[]

class KConfigurationTest {
    public fun `Starting an XTDB node from file`() {
        // tag::from-file[]
        val xtdbNode: IXtdb = IXtdb.startNode(File("config.json"))
        // end::from-file[]

        close(xtdbNode)
    }

    public fun `Starting an XTDB node from resource`() {
        // tag::from-resource[]
        val xtdbNode = IXtdb.startNode(MyApp::class.java.getResource("config.json"))
        // end::from-resource[]

        close(xtdbNode)
    }

    public fun `Starting an XTDB node with configurator`() {
        // tag::from-configurator[]
        val xtdbNode = IXtdb.startNode { n ->
            // ...
        }
        // end::from-configurator[]
    }

    public fun `Starting an XTDB node with http server`() {
        // tag::http-server[]
        val xtdbNode = IXtdb.startNode { n ->
            n.with("xtdb.http-server/server") { http ->
                http["port"] = 3000
            }
        }
        // end::http-server[]

        close(xtdbNode)
    }

    public fun `Starting an XTDB node and overriding a module implementation`() {
        // tag::override-module[]
        val xtdbNode = IXtdb.startNode { n ->
            n.with("xtdb/document-store") { docStore ->
                docStore.module("xtdb.s3/->document-store")
                docStore["bucket"] = "my-bucket"
                docStore["prefix"] = "my-prefix"
            }
        }
        // end::override-module[]

        close(xtdbNode)
    }

    public fun `Starting an XTDB node with nested modules`() {
        // tag::nested-modules-0[]
        val xtdbNode = IXtdb.startNode{ n ->
            n.with("xtdb/tx-log") { txLog ->
                txLog.with("kv-store") { kv ->
                    kv.module("xtdb.rocksdb/->kv-store")
                    kv["db-dir"] = File("/tmp/rocksdb")
                }
            }
            // end::nested-modules-0[]
            /*
            This obviously won't compile so has to be commented out
            // tag::nested-modules-1[]
            n.with("xtdb/document-store") { docStore -> ... }
            n.with("xtdb/index-store") { indexStore -> ... }
            // end::nested-modules-1[]
             */
            // tag::nested-modules-2[]
        }
        // end::nested-modules-2[]

        close(xtdbNode)
    }

    public fun `Starting an XTDB node with shared modules`() {
        // tag::sharing-modules[]
        val xtdbNode = IXtdb.startNode { n ->
            n.with("my-rocksdb") { rocks ->
                rocks.module("xtdb.rocksdb/->kv-store")
                rocks["db-dir"] = File("/tmp/rocksdb")
            }
            n.with("xtdb/document-store") { docStore ->
                docStore["kv-store"] = "my-rocksdb"
            }
            n.with("xtdb/tx-log") { txLog ->
                txLog["kv-store"] = "my-rocksdb"
            }
        }
        // end::sharing-modules[]

        close(xtdbNode)
    }

    private fun close(xtdbNode: IXtdb) {
        try {
            xtdbNode.close()
        } catch (e: IOException) {
            fail()
        }
    }
}
