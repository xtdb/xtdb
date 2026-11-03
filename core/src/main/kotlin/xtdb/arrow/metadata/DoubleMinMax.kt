package xtdb.arrow.metadata

class DoubleMinMax {
    var min = Double.POSITIVE_INFINITY
        private set

    var max = Double.NEGATIVE_INFINITY
        private set

    fun applyMinMax(value: Double) {
        min = min.coerceAtMost(value)
        max = max.coerceAtLeast(value)
    }
}
