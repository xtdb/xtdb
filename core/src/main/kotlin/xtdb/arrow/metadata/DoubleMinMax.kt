package xtdb.arrow.metadata

class DoubleMinMax {
    var min = Double.POSITIVE_INFINITY
        private set

    var max = Double.NEGATIVE_INFINITY
        private set

    fun applyMinMax(min: Double, max: Double) {
        this.min = this.min.coerceAtMost(min)
        this.max = this.max.coerceAtLeast(max)
    }
}
