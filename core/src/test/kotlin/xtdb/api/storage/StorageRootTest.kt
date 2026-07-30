package xtdb.api.storage

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import xtdb.api.error.Incorrect
import xtdb.api.storage.Storage.storageRoot
import xtdb.util.asPath

/**
 * Versions are passed literally rather than as [Storage.VERSION] throughout: the subject here is the
 * rendering of a root, which must stay stable across version bumps, not whatever version is current.
 */
class StorageRootTest {

    @Test
    fun `epoch 0 renders bare`() {
        assertEquals("v06".asPath, storageRoot(6, 0))
    }

    @Test
    fun `non-zero epoch appends a lex-hex suffix`() {
        assertEquals("v06_e01".asPath, storageRoot(6, 1))
        assertEquals("v06_e0a".asPath, storageRoot(6, 10))
    }

    /**
     * Asserted as equality with the two-arg root rather than against a literal, because the contract is
     * "indistinguishable from a store written before partitioning existed" — the two-arg form is the
     * reference for what those stores contain.
     */
    @Test
    fun `single partition is byte-identical to the unpartitioned root`() {
        for (epoch in listOf(0, 1, 10)) {
            assertEquals(storageRoot(6, epoch), storageRoot(6, epoch, 0, 1), "epoch $epoch")
        }
    }

    @Test
    fun `multiple partitions nest under a parts marker`() {
        assertEquals("parts/0/v06".asPath, storageRoot(6, 0, 0, 3))
        assertEquals("parts/2/v06".asPath, storageRoot(6, 0, 2, 3))
    }

    /**
     * Partition-outer, so each partition's subtree holds a complete set of epoch generations — which is
     * what keeps per-partition epochs open as a future option. The inverted nesting would foreclose it.
     */
    @Test
    fun `the partition marker sits outside the epoch root`() {
        assertEquals("parts/1/v06_e02".asPath, storageRoot(6, 2, 1, 2))
    }

    @Test
    fun `rejects partitions outside the declared range`() {
        assertThrows(Incorrect::class.java) { storageRoot(6, 0, 2, 2) }
        assertThrows(Incorrect::class.java) { storageRoot(6, 0, -1, 1) }
        assertThrows(Incorrect::class.java) { storageRoot(6, 0, 0, 0) }
    }
}
