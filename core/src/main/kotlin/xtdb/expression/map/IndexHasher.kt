package xtdb.expression.map

interface IndexHasher {
    fun hashCode(index: Int): Int
}