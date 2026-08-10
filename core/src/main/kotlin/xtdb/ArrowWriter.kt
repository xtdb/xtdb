package xtdb

import kotlinx.coroutines.runBlocking
import xtdb.trie.FileSize

interface ArrowWriter : AutoCloseable {
    fun writePage()

    /**
     * Writes out the file, returning its size.
     *
     * Suspends: for a remote buffer pool this is a network upload, and holding a dispatcher thread
     * for its duration is what turned one stalled S3 request into a node-wide stall (#5850).
     */
    suspend fun end(): FileSize

    /** For callers that aren't coroutine-native — see [end]. */
    fun endSync(): FileSize = runBlocking { end() }
}
