package xtdb.arrow

sealed class IntegerVector : NumericVector() {

    abstract fun getAsInt(idx: Int): Int
    abstract fun getAsLong(idx: Int): Long
}