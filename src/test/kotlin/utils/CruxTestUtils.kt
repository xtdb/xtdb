package utils

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import crux.api.CruxDocument
import crux.api.ICruxAPI
import crux.api.ICruxDatasource
import crux.api.TransactionInstant
import crux.api.tx.Transaction
import crux.api.tx.Transaction.buildTx
import org.junit.jupiter.api.Assertions.assertNull
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

private fun ICruxDatasource.assertDocument(document: CruxDocument) =
    assertThat(
        entity(document.id),
        equalTo(document)
    )

fun ICruxAPI.assertDocument(document: CruxDocument) =
    db().assertDocument(document)

fun ICruxAPI.assertDocument(document: CruxDocument, validTime: Date) =
    db(validTime).assertDocument(document)

fun ICruxAPI.assertDocument(document: CruxDocument, validTime: Date, transactionTime: Date) =
    db(validTime, transactionTime).assertDocument(document)

private fun ICruxDatasource.assertNoDocument(id: Any) =
    assertNull(entity(id))

fun ICruxAPI.assertNoDocument(id: Any) =
    db().assertNoDocument(id)

fun ICruxAPI.assertNoDocument(id: Any, validTime: Date) =
    db(validTime).assertNoDocument(id)

fun ICruxAPI.assertNoDocument(id: Any, validTime: Date, transactionTime: Date) =
    db(validTime, transactionTime).assertNoDocument(id)
