package xtdb.util

import kotlin.random.Random

class SkipListNode<T>(
    var value: T?,
    val level: Int
) {
    val next = Array<SkipListNode<T>?>(level) { null }
    val span = IntArray(level) { 1 }  // Distance to the next node at each level
}

class SkipList<T : Comparable<T>> : Comparable<SkipList<T>> {
    private val MAX_LEVEL = 32
    private val P = 0.5

    private val head = SkipListNode<T>(null, MAX_LEVEL)
    private var level = 1
    var size = 0
    private val rand = Random(System.currentTimeMillis())

    private fun randomLevel(): Int {
        var lvl = 1
        while (rand.nextDouble() < P && lvl < MAX_LEVEL) {
            lvl++
        }
        return lvl
    }

    operator fun get(index: Int): T {
        val node = getNode(index)
        return node.value ?: throw IndexOutOfBoundsException()
    }

    operator fun set(index: Int, value: T) {
        val node = getNode(index)
        node.value = value
    }

    private fun getNode(index: Int): SkipListNode<T> {
        if (index < 0 || index >= size) throw IndexOutOfBoundsException()
        var x = head
        var i = -1  // Start from position -1 because head is before the first element
        for (k in level - 1 downTo 0) {
            while (x.next[k] != null && i + x.span[k] <= index) {
                i += x.span[k]
                x = x.next[k]!!
            }
        }
        return x
    }

    fun add(value: T) {
        insert(size, value)
    }

    fun insert(pos: Int, value: T) {
        if (pos < 0 || pos > size) throw IndexOutOfBoundsException("Index out of bounds: $pos")
        val update = Array<SkipListNode<T>>(MAX_LEVEL) { head }
        val rank = IntArray(MAX_LEVEL) { 0 }

        var x = head
        for (k in level - 1 downTo 0) {
            rank[k] = if (k == level - 1) 0 else rank[k + 1]
            while (x.next[k] != null && rank[k] + x.span[k] < pos + 1) {
                rank[k] += x.span[k]
                x = x.next[k]!!
            }
            update[k] = x
        }

        val lvl = randomLevel()
        if (lvl > level) {
            for (k in level until lvl) {
                update[k] = head
                update[k].span[k] = size + 1
            }
            level = lvl
        }

        val newNode = SkipListNode(value, lvl)
        for (k in 0 until lvl) {
            newNode.next[k] = update[k].next[k]
            update[k].next[k] = newNode

            newNode.span[k] = update[k].span[k] - (pos - rank[k])
            update[k].span[k] = (pos - rank[k]) + 1
        }

        for (k in lvl until level) {
            update[k].span[k]++
        }

        size++
    }

    fun clear() {
        for (k in 0 until level) {
            head.next[k] = null
            head.span[k] = 1
        }
        level = 1
        size = 0
    }

    fun removeRange(fromIndex: Int, toIndex: Int) {
        if (fromIndex < 0 || toIndex > size) {
            throw IndexOutOfBoundsException("Invalid range: $fromIndex to $toIndex")
        }
        val update = Array<SkipListNode<T>>(MAX_LEVEL) { head }
        val rank = IntArray(MAX_LEVEL) { 0 }

        var x = head
        for (k in level - 1 downTo 0) {
            rank[k] = if (k == level - 1) 0 else rank[k + 1]
            while (x.next[k] != null && rank[k] + x.span[k] < fromIndex + 1) {
                rank[k] += x.span[k]
                x = x.next[k]!!
            }
            update[k] = x
        }

        var nodesToRemove = toIndex - fromIndex
        for (k in 0 until level) {
            if (update[k].next[k] != null) {
                var node = update[k].next[k]
                var spanSum = 0
                while (node != null && spanSum < nodesToRemove) {
                    spanSum += node.span[k]
                    node = node.next[k]
                }
                update[k].next[k] = node
                update[k].span[k] += spanSum - nodesToRemove
            } else {
                update[k].span[k] -= nodesToRemove
            }
        }

        while (level > 1 && head.next[level - 1] == null) {
            level--
        }

        size -= nodesToRemove
    }

    override fun compareTo(other: SkipList<T>): Int {
        var node1 = this.head.next[0]
        var node2 = other.head.next[0]
        while (node1 != null && node2 != null) {
            val cmp = node1.value!!.compareTo(node2.value!!)
            if (cmp != 0) {
                return cmp
            }
            node1 = node1.next[0]
            node2 = node2.next[0]
        }
        return when {
            node1 != null -> 1
            node2 != null -> -1
            else -> 0
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SkipList<*>) return false
        return this.compareTo(other as SkipList<T>) == 0
    }

    override fun toString(): String {
        val sb = StringBuilder()
        var node = head.next[0]
        sb.append("[")
        while (node != null) {
            sb.append(node.value)
            node = node.next[0]
            if (node != null) {
                sb.append(", ")
            }
        }
        sb.append("]")
        return sb.toString()
    }

    companion object {
        fun <T: Comparable<T>> from(vararg elements: T): SkipList<T>{
            val skipList = SkipList<T>()
            for (element in elements) {
                skipList.add(element)
            }
            return skipList
        }
    }
}