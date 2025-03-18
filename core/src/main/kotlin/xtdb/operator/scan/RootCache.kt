package xtdb.operator.scan

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import xtdb.BufferPool
import xtdb.util.closeAll
import java.nio.file.Path
import java.util.*

class RootCache(private val al: BufferAllocator, private val bp: BufferPool) : AutoCloseable {
    private val freeRoots = mutableMapOf<Path, Stack<VectorSchemaRoot>>()
    private val usedRoots = mutableMapOf<Path, Stack<VectorSchemaRoot>>()

    fun openRoot(path: Path): VectorSchemaRoot {
        val root = freeRoots[path]?.takeUnless { it.isEmpty() }?.pop()
            ?: VectorSchemaRoot.create(bp.getFooter(path).schema, al)

        usedRoots.compute(path) { _, stack -> (stack ?: Stack()).also { it.push(root) } }

        return root
    }

    fun reset() {
        usedRoots.forEach { (path, roots) ->
            freeRoots.compute(path) { _, freeRoots -> freeRoots?.also { it.addAll(roots) } ?: roots }
        }

        usedRoots.clear()
    }

    override fun close() {
        freeRoots.values.forEach { it.closeAll() }
        usedRoots.values.forEach { it.closeAll() }
    }
}
