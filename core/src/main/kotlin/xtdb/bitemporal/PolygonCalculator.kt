package xtdb.bitemporal

import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.trie.EventRowPointer
import xtdb.util.TemporalBounds

class PolygonCalculator(private val queryBounds: TemporalBounds? = null) {
    companion object {
        private fun ArrowBufPointer.setFrom(src: ArrowBufPointer) = apply { set(src.buf, src.offset, src.length) }
    }

    private val skipIidPtr = ArrowBufPointer()
    private val prevIidPtr = ArrowBufPointer()
    val currentIidPtr = ArrowBufPointer()

    private val ceiling = Ceiling()
    private val polygon = Polygon()

    fun reset() = ceiling.reset()

    fun calculate(erp: EventRowPointer): Polygon? {
        // after an erase, we don't process any more rows
        if (skipIidPtr == erp.getIidPointer(currentIidPtr)) return null

        if (prevIidPtr != currentIidPtr) {
            ceiling.reset()
            prevIidPtr.setFrom(currentIidPtr)
        }

        val isErase = erp.op == "erase"

        val systemFrom = erp.systemFrom

        // unless it's an erase, we don't want to take any events after the query's snapshot time into account.
        if (!isErase && queryBounds != null && systemFrom >= queryBounds.systemTime.upper) return null

        val validFrom = erp.validFrom
        val validTo = erp.validTo

        polygon.calculateFor(ceiling, validFrom, validTo)
        ceiling.applyLog(systemFrom, validFrom, validTo)

        if (isErase) {
            skipIidPtr.setFrom(currentIidPtr)
        }
        
        return polygon
    }
}