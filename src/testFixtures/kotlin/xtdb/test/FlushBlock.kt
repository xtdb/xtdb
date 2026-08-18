package xtdb.test

import xtdb.XtdbInternal
import xtdb.api.Xtdb
import java.time.Duration

/**
 * Finishes a block on every attached database and waits for it, so a test can put data on the far side of
 * the historical/live boundary. Rows written before the flush are read from tries; rows written after are
 * read from the live index, and several behaviours differ across that line.
 *
 * The Clojure counterpart is `tu/flush-block!`.
 */
@JvmOverloads
fun Xtdb.flushBlock(timeout: Duration = Duration.ofSeconds(5)) {
    (this as XtdbInternal).dbCatalog.let { cat ->
        cat.databaseNames.mapNotNull { cat.databaseOrNull(it) }.forEach { it.sendFlushBlockMessage() }
        cat.syncAll(timeout)
    }
}
