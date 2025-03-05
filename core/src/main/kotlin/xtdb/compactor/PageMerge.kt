package xtdb.compactor

import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.arrow.Relation
import xtdb.arrow.RelationReader
import xtdb.arrow.RowCopier
import xtdb.bitemporal.PolygonCalculator
import xtdb.trie.EventRowPointer
import xtdb.trie.HashTrie
import xtdb.trie.MergePlanNode
import xtdb.trie.MergePlanTask
import java.util.*

internal class PageMerge(private val dataRel: Relation, private val pathFilter: ByteArray?) {
    private val iidWtr = dataRel["_iid"]!!
    private val sfWtr = dataRel["_system_from"]!!
    private val vfWtr = dataRel["_valid_from"]!!
    private val vtWtr = dataRel["_valid_to"]!!
    private val opWtr = dataRel["op"]!!

    private val polygonCalculator = PolygonCalculator()
    private val isValidPtr = ArrowBufPointer()

    private fun rowCopier(dataReader: RelationReader): RowCopier {
        val iidCopier = dataReader["_iid"]!!.rowCopier(iidWtr)
        val sfCopier = dataReader["_system_from"]!!.rowCopier(sfWtr)
        val vfCopier = dataReader["_valid_from"]!!.rowCopier(vfWtr)
        val vtCopier = dataReader["_valid_to"]!!.rowCopier(vtWtr)
        val opCopier = dataReader["op"]!!.rowCopier(opWtr)

        return RowCopier {
            val pos = iidCopier.copyRow(it)

            sfCopier.copyRow(it)
            vfCopier.copyRow(it)
            vtCopier.copyRow(it)
            opCopier.copyRow(it)
            dataRel.endRow()

            pos
        }
    }

    data class QueueElem(val evPtr: EventRowPointer, val rowCopier: RowCopier)

    @Suppress("UNCHECKED_CAST")
    private fun <L : HashTrie.Node<*>> MergePlanNode<L>.loadDataPage(): RelationReader? =
        segment.dataRel?.loadPage(node as L)

    fun mergePages(task: MergePlanTask) {
        val mpNodes = task.mpNodes
        var path = task.path
        val dataReaders = mpNodes.map { it.loadDataPage() }
        val mergeQueue = PriorityQueue(Comparator.comparing(QueueElem::evPtr, EventRowPointer.comparator()))
        path = if (pathFilter == null || path.size > pathFilter.size) path else pathFilter

        for (dataReader in dataReaders) {
            if (dataReader == null) continue
            val evPtr = EventRowPointer.XtArrow(dataReader, path)
            val rowCopier = rowCopier(dataReader)
            if (evPtr.isValid(isValidPtr, path)) {
                mergeQueue.add(QueueElem(evPtr, rowCopier))
            }
        }

        var seenErase = false

        while (true) {
            val elem = mergeQueue.poll() ?: break
            val (evPtr, rowCopier) = elem
            if (polygonCalculator.calculate(evPtr) != null) {
                rowCopier.copyRow(evPtr.index)
            } else {
                if (!seenErase) rowCopier.copyRow(evPtr.index)
                seenErase = true
            }

            evPtr.nextIndex()
            if (evPtr.isValid(isValidPtr, path)) {
                mergeQueue.add(elem)
            }
        }
    }
}