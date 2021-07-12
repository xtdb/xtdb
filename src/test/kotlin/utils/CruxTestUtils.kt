package utils

import crux.api.CruxDocument
import crux.api.ICruxAPI
import crux.api.TransactionInstant
import crux.api.tx.Transaction
import crux.api.tx.Transaction.buildTx
import java.time.Duration
import java.util.*

fun aDocument(): CruxDocument = CruxDocument.build(
    UUID.randomUUID().toString()
) {
    it.put("Name", UUID.randomUUID().toString())
}

private fun ICruxAPI.performAndWait(transaction: Transaction): TransactionInstant =
    awaitTx(
        submitTx(transaction),
        Duration.ofSeconds(10)
    )

fun ICruxAPI.putAndWait(document: CruxDocument) =
    performAndWait(
        buildTx {
            it.put(document)
        }
    )

fun ICruxAPI.putAndWait(document: CruxDocument, validTime: Date) =
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