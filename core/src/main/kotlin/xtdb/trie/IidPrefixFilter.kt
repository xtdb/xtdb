package xtdb.trie

import java.util.function.Predicate

/**
 * Trie paths that can hold an IID whose 32-bit prefix agrees with one of [values] on the bits of [mask].
 *
 * A path is compared only on the masked bits of the levels it has, down to the end of the prefix: deeper levels, and unmasked bits, are free.
 * It's a superset filter — every path holding a matching IID passes, and a passing path may hold none.
 */
class IidPrefixFilter(private val mask: Int, private val values: IntArray) : Predicate<ByteArray> {
    init {
        require(mask != 0 && values.isNotEmpty()) { "a prefix filter pins some bits to at least one value" }
    }

    override fun test(path: ByteArray): Boolean {
        val levels = minOf(path.size, Int.SIZE_BITS / DEFAULT_LEVEL_BITS)
        if (levels == 0) return true

        var pathBits = 0
        for (level in 0 until levels)
            pathBits = pathBits or (path[level].toInt() shl (Int.SIZE_BITS - DEFAULT_LEVEL_BITS * (level + 1)))

        val pathMask = mask and (-1 shl (Int.SIZE_BITS - DEFAULT_LEVEL_BITS * levels))
        return values.any { ((it xor pathBits) and pathMask) == 0 }
    }
}
