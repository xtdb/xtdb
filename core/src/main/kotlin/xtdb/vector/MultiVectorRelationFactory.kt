package xtdb.vector

import com.carrotsearch.hppc.IntArrayList
import org.apache.arrow.vector.NullVector
import xtdb.arrow.RelationReader
import xtdb.arrow.VectorIndirection.Companion.selection
import xtdb.arrow.VectorReader
import xtdb.trie.ColumnName

class MultiVectorRelationFactory(leafRels: List<RelationReader>, colNames: List<ColumnName>) {

    private val readers: Map<ColumnName, List<VectorReader>>

    init {
        val putReaders = leafRels.map { it["op"].vectorForOrNull("put") }

        readers = colNames.associateWith { colName ->
            if (colName == "_iid") {
                leafRels.map { it["_iid"] }
            } else {
                putReaders.filterNotNull().map { putReader ->
                    putReader.vectorForOrNull(colName)?.withName(colName)
                        ?: ValueVectorReader.from(NullVector(colName, putReader.valueCount))
                }
            }
        }
    }

    private val readerIndirection = IntArrayList()
    private val idxIndirection = IntArrayList()

    fun accept(readerIdx: Int, rowIdx: Int) {
        readerIndirection.add(readerIdx)
        idxIndirection.add(rowIdx)
    }

    fun realize(): RelationReader {
        val readerSelection = selection(readerIndirection.toArray())
        val idxSelection = selection(idxIndirection.toArray())

        return RelationReader.from(
            readers.map {
                IndirectMultiVectorReader(
                    it.key, it.value, readerSelection, idxSelection
                )
            },
            idxSelection.valueCount()
        )
    }
}