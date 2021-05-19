package crux.api

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import crux.api.Crux.startNode
import crux.api.ModuleConfiguration.buildModule
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import utils.aDocument
import utils.assertDocument
import utils.putAndWait
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
    inner class BaseJavaCrux {
        /**
         * This is for testing that all is well with imports and providing a comparison with our new DSL
          */

        @Test
        fun `Can create an in memory node`() =
            startNode().use { node ->
                val document = aDocument()
                node.putAndWait(document)
                node.assertDocument(document)
            }

        @Test
        fun `Can use RocksDB for persistence`(@TempDir tempPath: Path) {
            val (tx, doc, index) = createTestFolders(tempPath)
            val document = aDocument()

            startNode { n ->
                n.with("crux/tx-log") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("crux.rocksdb/->kv-store")
                        kv.set("db-dir", tx)
                    })
                }
                n.with("crux/document-store") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("crux.rocksdb/->kv-store")
                        kv.set("db-dir", doc)
                    })
                }
                n.with("crux/index-store") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("crux.rocksdb/->kv-store")
                        kv.set("db-dir", index)
                    })
                }
            }.use {
                it.putAndWait(document)
            }

            startNode { n ->
                n.with("crux/tx-log") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("crux.rocksdb/->kv-store")
                        kv.set("db-dir", tx)
                    })
                }
                n.with("crux/document-store") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("crux.rocksdb/->kv-store")
                        kv.set("db-dir", doc)
                    })
                }
                n.with("crux/index-store") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("crux.rocksdb/->kv-store")
                        kv.set("db-dir", index)
                    })
                }
            }.use {
                it.assertDocument(document)
            }
        }
    }

    @Nested
    inner class KotlinDSL {
        @Test
        fun `Can create an in memory node`() =
            CruxK.startNode().use { node ->
                val document = aDocument()
                node.putAndWait(document)
                node.assertDocument(document)
            }

        @Test
        fun `Can use RocksDB for persistence`(@TempDir tempPath: Path) {
            val (tx, doc, index) = createTestFolders(tempPath)
            val document = aDocument()

            CruxK.startNode {
                "crux/tx-log" {
                    "kv-store" {
                        module = "crux.rocksdb/->kv-store"
                        "db-dir" to tx
                    }
                }
                "crux/document-store" {
                    "kv-store" {
                        module = "crux.rocksdb/->kv-store"
                        "db-dir" to doc
                    }
                }
                "crux/index-store" {
                    "kv-store" {
                        module = "crux.rocksdb/->kv-store"
                        "db-dir" to index
                    }
                }
            }.use {
                it.putAndWait(document)
            }

            CruxK.startNode {
                "crux/tx-log" {
                    "kv-store" {
                        module = "crux.rocksdb/->kv-store"
                        "db-dir" to tx
                    }
                }
                "crux/document-store" {
                    "kv-store" {
                        module = "crux.rocksdb/->kv-store"
                        "db-dir" to doc
                    }
                }
                "crux/index-store" {
                    "kv-store" {
                        module = "crux.rocksdb/->kv-store"
                        "db-dir" to index
                    }
                }
            }.use {
                it.assertDocument(document)
            }
        }
    }

    @Nested
    inner class UnitComparisons {
        @Test
        fun `Check that Kotlin DSL results in the same output as using Java Builders`(@TempDir tempPath: Path) {
            val (tx, doc, index) = createTestFolders(tempPath)

            val java = NodeConfiguration.buildNode { n ->
                n.with("crux/tx-log") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("crux.rocksdb/->kv-store")
                        kv.set("db-dir", tx)
                    })
                }
                n.with("crux/document-store") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("crux.rocksdb/->kv-store")
                        kv.set("db-dir", doc)
                    })
                }
                n.with("crux/index-store") { m ->
                    m.with("kv-store", buildModule { kv ->
                        kv.module("crux.rocksdb/->kv-store")
                        kv.set("db-dir", index)
                    })
                }
            }

            val kotlin = NodeConfigurationContext.build {
                "crux/tx-log" {
                    "kv-store" {
                        module = "crux.rocksdb/->kv-store"
                        "db-dir" to tx
                    }
                }
                "crux/document-store" {
                    "kv-store" {
                        module = "crux.rocksdb/->kv-store"
                        "db-dir" to doc
                    }
                }
                "crux/index-store" {
                    "kv-store" {
                        module = "crux.rocksdb/->kv-store"
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
