package utils

import xtdb.api.IXtdb
import xtdb.api.TransactionInstant
import xtdb.api.XtdbDocument
import xtdb.api.tx.Transaction
import xtdb.api.tx.Transaction.buildTx
import java.time.Duration
import java.util.*

fun aDocument(): XtdbDocument = XtdbDocument.build(
    UUID.randomUUID().toString()
) {
    it.put("Name", UUID.randomUUID().toString())
}

private fun IXtdb.performAndWait(transaction: Transaction): TransactionInstant =
    awaitTx(
        submitTx(transaction),
        Duration.ofSeconds(10)
    )

fun IXtdb.putAndWait(document: XtdbDocument) =
    performAndWait(
        buildTx {
            it.put(document)
        }
    )

fun IXtdb.putAndWait(document: XtdbDocument, validTime: Date) =
    performAndWait(
        buildTx {
            it.put(document, validTime)
        }
    )

fun MutableCollection<MutableList<*>>.singleResultSet() = map { it[0] }.toSet()
fun MutableCollection<MutableList<*>>.simplifiedResultSet() = map { it.toList() }.toSet()
fun MutableCollection<MutableList<*>>.singleResultList() = map { it[0] }
fun MutableCollection<MutableList<*>>.simplifiedResultList() = map { it.toList() }
fun MutableCollection<MutableList<*>>.singleResult() = singleResultSet().single()