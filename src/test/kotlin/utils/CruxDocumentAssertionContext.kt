package utils

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import crux.api.CruxDocument
import crux.api.ICruxAPI
import crux.api.ICruxDatasource
import org.junit.jupiter.api.Assertions
import java.util.*

class CruxDocumentAssertionContext(private val crux: ICruxAPI) {
    infix fun CruxDocument.at(validTime: Date) = crux.assertDocument(this, validTime)
    infix fun CruxDocument.notAt(validTime: Date) = crux.assertNoDocument(id, validTime)

    operator fun CruxDocument.unaryPlus() = crux.assertDocument(this)
    operator fun CruxDocument.unaryMinus() = crux.assertNoDocument(id)

    inner class CruxDocumentValidTimeAssertionContext(private val validTime: Date) {
        operator fun CruxDocument.unaryPlus() = crux.assertDocument(this, validTime)
        operator fun CruxDocument.unaryMinus() = crux.assertNoDocument(id, validTime)
    }

    fun at(validTime: Date, block: CruxDocumentValidTimeAssertionContext.() -> Unit) =
        CruxDocumentValidTimeAssertionContext(validTime).also(block)
}

fun ICruxAPI.assert(block: CruxDocumentAssertionContext.() -> Unit) =
    CruxDocumentAssertionContext(this).run(block)

private fun ICruxDatasource.assertDocument(document: CruxDocument) =
    assertThat(
        entity(document.id),
        equalTo(document)
    )

private fun ICruxAPI.assertDocument(document: CruxDocument) =
    db().assertDocument(document)

private fun ICruxAPI.assertDocument(document: CruxDocument, validTime: Date) =
    db(validTime).assertDocument(document)

private fun ICruxDatasource.assertNoDocument(id: Any) =
    Assertions.assertNull(entity(id))

private fun ICruxAPI.assertNoDocument(id: Any) =
    db().assertNoDocument(id)

private fun ICruxAPI.assertNoDocument(id: Any, validTime: Date) =
    db(validTime).assertNoDocument(id)