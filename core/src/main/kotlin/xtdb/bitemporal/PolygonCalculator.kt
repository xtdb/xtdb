package xtdb.bitemporal

import org.apache.arrow.memory.util.ArrowBufPointer
import xtdb.trie.EventRowPointer
import xtdb.util.TemporalBounds

class PolygonCalculator(private val temporalBounds: TemporalBounds? = null) {
    companion object {
        private fun ArrowBufPointer.setFrom(src: ArrowBufPointer) = apply { set(src.buf, src.offset, src.length) }
    }

    private val skipIidPtr = ArrowBufPointer()
    private val prevIidPtr = ArrowBufPointer()
    private val currentIidPtr = ArrowBufPointer()

    private val ceiling = Ceiling()
    private val polygon = Polygon()

    fun calculate(erp: EventRowPointer): Polygon? {
        if (skipIidPtr == erp.getIidPointer(currentIidPtr)) return null

        if (prevIidPtr != currentIidPtr) {
            ceiling.reset()
            prevIidPtr.setFrom(currentIidPtr)
        }

        if (erp.op == "erase") {
            ceiling.reset()
            skipIidPtr.setFrom(currentIidPtr)
            return null
        }

        val systemFrom = erp.systemFrom

        if (temporalBounds != null && systemFrom >= temporalBounds.systemTime.upper) return null

        val validFrom = erp.validFrom
        val validTo = erp.validTo

        polygon.calculateFor(ceiling, validFrom, validTo)
        ceiling.applyLog(systemFrom, validFrom, validTo)
        return polygon
    }
}