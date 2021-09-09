package xtdb.api

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import utils.aDocument
import utils.assert
import utils.putAndWait
import xtdb.api.IXtdb.startNode
import xtdb.api.ModuleConfiguration.buildModule
import java.nio.file.Path

class ConfigurationTest {
    companion object {
        private fun createTestFolders(path: Path) =
            Triple(
                path.resolve("tx").toFile(),
                path.resolve("docs").toFile(),
                path.resolve("index").toFile()
            )
    }

    @Nested
    inner class BaseJavaXtdb {
        /**
         * This is for testing that all is well with imports and providing a comparison with our new DSL
          */

        @Test
        fun `Can create an in memory node`() =
            startNode().use { node ->
                val document = aDocument()
                node.putAndWait(document)
                node.assert {
                    + document
                }
            }

        @Test
        fun `Can use RocksDB for persistence`(@TempDir tempPath: Path) {
            val (tx, doc, index) = createTestFolders(tempPath)
            val document = aDocument()

            startNode { n ->
                n.with("xtdb/tx-log") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("xtdb.rocksdb/->kv-store")
                        kv.set("db-dir", tx)
                    })
                }
                n.with("xtdb/document-store") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("xtdb.rocksdb/->kv-store")
                        kv.set("db-dir", doc)
                    })
                }
                n.with("xtdb/index-store") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("xtdb.rocksdb/->kv-store")
                        kv.set("db-dir", index)
                    })
                }
            }.use {
                it.putAndWait(document)
            }

            startNode { n ->
                n.with("xtdb/tx-log") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("xtdb.rocksdb/->kv-store")
                        kv.set("db-dir", tx)
                    })
                }
                n.with("xtdb/document-store") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("xtdb.rocksdb/->kv-store")
                        kv.set("db-dir", doc)
                    })
                }
                n.with("xtdb/index-store") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("xtdb.rocksdb/->kv-store")
                        kv.set("db-dir", index)
                    })
                }
            }.use {
                it.assert {
                    + document
                }
            }
        }
    }

    @Nested
    inner class KotlinDSL {
        @Test
        fun `Can create an in memory node`() =
            XtdbKt.startNode().use { node ->
                val document = aDocument()
                node.putAndWait(document)
                node.assert {
                    + document
                }
            }

        @Test
        fun `Can use RocksDB for persistence`(@TempDir tempPath: Path) {
            val (tx, doc, index) = createTestFolders(tempPath)
            val document = aDocument()

            XtdbKt.startNode {
                "xtdb/tx-log" {
                    "kv-store" {
                        module = "xtdb.rocksdb/->kv-store"
                        "db-dir" to tx
                    }
                }
                "xtdb/document-store" {
                    "kv-store" {
                        module = "xtdb.rocksdb/->kv-store"
                        "db-dir" to doc
                    }
                }
                "xtdb/index-store" {
                    "kv-store" {
                        module = "xtdb.rocksdb/->kv-store"
                        "db-dir" to index
                    }
                }
            }.use {
                it.putAndWait(document)
            }

            XtdbKt.startNode {
                "xtdb/tx-log" {
                    "kv-store" {
                        module = "xtdb.rocksdb/->kv-store"
                        "db-dir" to tx
                    }
                }
                "xtdb/document-store" {
                    "kv-store" {
                        module = "xtdb.rocksdb/->kv-store"
                        "db-dir" to doc
                    }
                }
                "xtdb/index-store" {
                    "kv-store" {
                        module = "xtdb.rocksdb/->kv-store"
                        "db-dir" to index
                    }
                }
            }.use {
                it.assert {
                    + document
                }
            }
        }
    }

    @Nested
    inner class UnitComparisons {
        @Test
        fun `Check that Kotlin DSL results in the same output as using Java Builders`(@TempDir tempPath: Path) {
            val (tx, doc, index) = createTestFolders(tempPath)

            val java = NodeConfiguration.buildNode { n ->
                n.with("xtdb/tx-log") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("xtdb.rocksdb/->kv-store")
                        kv.set("db-dir", tx)
                    })
                }
                n.with("xtdb/document-store") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("xtdb.rocksdb/->kv-store")
                        kv.set("db-dir", doc)
                    })
                }
                n.with("xtdb/index-store") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("xtdb.rocksdb/->kv-store")
                        kv.set("db-dir", index)
                    })
                }
            }

            val kotlin = NodeConfigurationContext.build {
                "xtdb/tx-log" {
                    "kv-store" {
                        module = "xtdb.rocksdb/->kv-store"
                        "db-dir" to tx
                    }
                }
                "xtdb/document-store" {
                    "kv-store" {
                        module = "xtdb.rocksdb/->kv-store"
                        "db-dir" to doc
                    }
                }
                "xtdb/index-store" {
                    "kv-store" {
                        module = "xtdb.rocksdb/->kv-store"
                        "db-dir" to index
                    }
                }
            }

            assertThat(
                java,
                equalTo(kotlin)
            )
        }
    }
}
