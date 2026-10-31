package xtdb.trie

internal class LeafPointer(val evPtr: EventRowPointer, val relIdx: Int)

/**
 * A binary min-heap of page pointers in [EventRowPointer.compare] order, so its top is the next event to serve.
 * A caller advances the top pointer in place, then calls [reseatTop].
 *
 * Each pointer is one page's cursor, sitting on that page's next unserved event.
 * A page's rows are already in [EventRowPointer.compare] order, so the least of the pointers is the least unserved event across every page.
 */
internal class LeafHeap(pointers: List<LeafPointer>) {
    // The array-backed binary heap (CLRS, ch. 6): `heap[0 until size]` is the tree, `heap[i]`'s children sit at `2i + 1` and `2i + 2`, and no pointer precedes its parent.
    // A page that runs out is dropped: the last pointer takes its slot, and `size` shrinks by one.
    private val heap = pointers.filter { it.evPtr.isValid() }.toTypedArray()
    private var size = heap.size

    // Floyd's bottom-up heap construction: sift down every parent, deepest first.
    init {
        for (i in size / 2 - 1 downTo 0) siftDown(i)
    }

    fun top(): LeafPointer? = if (size == 0) null else heap[0]

    // Advancing a page's pointer only ever moves it later in the order, so sifting the root down restores the heap without a sift up.
    fun reseatTop() {
        if (!heap[0].evPtr.isValid()) heap[0] = heap[--size]
        if (size > 0) siftDown(0)
    }

    // Carries `node` down as a hole, moving each preceding child up a level, and writes it once where neither child precedes it.
    private fun siftDown(from: Int) {
        val node = heap[from]
        var i = from

        while (true) {
            var child = 2 * i + 1
            if (child >= size) break
            if (child + 1 < size && precedes(heap[child + 1], heap[child])) child++
            if (!precedes(heap[child], node)) break

            heap[i] = heap[child]
            i = child
        }

        heap[i] = node
    }

    private fun precedes(l: LeafPointer, r: LeafPointer) = EventRowPointer.compare(l.evPtr, r.evPtr) < 0
}
