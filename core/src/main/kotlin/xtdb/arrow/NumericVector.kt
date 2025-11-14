package xtdb.arrow

sealed class NumericVector : FixedWidthVector() {

    abstract fun getAsFloat(idx: Int): Float
    abstract fun getAsDouble(idx: Int): Double
}