package xtdb

import xtdb.compactor.Compactor
import xtdb.log.proto.TrieDetails
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
import xtdb.table.TableRef
import xtdb.trie.Trie.dataFilePath
import xtdb.trie.Trie.metaFilePath
import xtdb.trie.TrieKey
import xtdb.util.StringUtil.asLexHex
import xtdb.util.requiringResolve
import java.nio.ByteBuffer

class SimulationTestUtils {
    companion object {
        fun buildTrieDetails(tableName: String, trieKey: String, dataFileSize: Long = 1024L): TrieDetails =
            TrieDetails.newBuilder()
                .setTableName(tableName)
                .setTrieKey(trieKey)
                .setDataFileSize(dataFileSize)
                .build()

        fun addTriesToBufferPool(bufferPool: BufferPool, tableRef: TableRef, tries: List<TrieDetails>) {
            tries.forEach { trie ->
                val trieKey = trie.trieKey
                bufferPool.putObject(tableRef.dataFilePath(trieKey), ByteBuffer.allocate(1))
                bufferPool.putObject(tableRef.metaFilePath(trieKey), ByteBuffer.allocate(1))
            }
        }

        // Clojure interop to get at internal functions
        val setLogLevel = requiringResolve("xtdb.logging/set-log-level!")
        private val createJobCalculatorFn = requiringResolve("xtdb.compactor.job-calculator/->JobCalculator")
        private val createTrieCatalogFn = requiringResolve("xtdb.trie-catalog/->TrieCatalog")

        fun createJobCalculator(): Compactor.JobCalculator =
            createJobCalculatorFn.invoke() as Compactor.JobCalculator

        fun createTrieCatalog(): TrieCatalog =
            createTrieCatalogFn.invoke(null, null, mutableMapOf<Any, Any>(), 100 * 1024 * 1024) as TrieCatalog

        val L0TrieKeys = sequence {
            var blockIndex = 0
            while (true) {
                yield("l00-rc-b" + blockIndex.asLexHex)
                blockIndex++
            }
        }

        val L1TrieKeys = sequence {
            var blockIndex = 0
            while (true) {
                yield("l01-rc-b" + blockIndex.asLexHex)
                blockIndex++
            }
        }

        val L2TrieKeys = sequence {
            var partition = 0
            var blockIndex = 0
            while (true) {
                yield("l02-rc-p$partition-b" + blockIndex.asLexHex)
                partition = (partition + 1) % 4
                if (partition == 0) blockIndex++
            }
        }

        val L3TrieKeys = sequence {
            var partition = 0
            var blockIndex = 0
            while (true) {
                yield("l03-rc-p0$partition-b" + blockIndex.asLexHex)
                partition = (partition + 1) % 4
                if (partition == 0) blockIndex++
            }
        }

        fun List<TrieKey>.prefix(levelPrefix: String) = this.filter { it.startsWith(levelPrefix) }
    }
}
