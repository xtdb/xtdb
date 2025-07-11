package xtdb.expression.map

fun interface IndexHasher {
    fun hashCode(index: Int): Int
}
