package utils

import com.natpryce.hamkrest.assertion.assertThat
import com.natpryce.hamkrest.equalTo
import org.junit.jupiter.api.Assertions
import xtdb.api.IXtdb
import xtdb.api.IXtdbDatasource
import xtdb.api.XtdbDocument
import java.util.*

class XtdbDocumentAssertionContext(private val xtdb: IXtdb) {
    infix fun XtdbDocument.at(validTime: Date) = xtdb.assertDocument(this, validTime)
    infix fun XtdbDocument.notAt(validTime: Date) = xtdb.assertNoDocument(id, validTime)

    operator fun XtdbDocument.unaryPlus() = xtdb.assertDocument(this)
    operator fun XtdbDocument.unaryMinus() = xtdb.assertNoDocument(id)

    inner class XtdbDocumentValidTimeAssertionContext(private val validTime: Date) {
        operator fun XtdbDocument.unaryPlus() = xtdb.assertDocument(this, validTime)
        operator fun XtdbDocument.unaryMinus() = xtdb.assertNoDocument(id, validTime)
    }

    fun at(validTime: Date, block: XtdbDocumentValidTimeAssertionContext.() -> Unit) =
        XtdbDocumentValidTimeAssertionContext(validTime).also(block)
}

fun IXtdb.assert(block: XtdbDocumentAssertionContext.() -> Unit) =
    XtdbDocumentAssertionContext(this).run(block)

private fun IXtdbDatasource.assertDocument(document: XtdbDocument) =
    assertThat(
        entity(document.id),
        equalTo(document)
    )

private fun IXtdb.assertDocument(document: XtdbDocument) =
    db().assertDocument(document)

private fun IXtdb.assertDocument(document: XtdbDocument, validTime: Date) =
    db(validTime).assertDocument(document)

private fun IXtdbDatasource.assertNoDocument(id: Any) =
    Assertions.assertNull(entity(id))

private fun IXtdb.assertNoDocument(id: Any) =
    db().assertNoDocument(id)

private fun IXtdb.assertNoDocument(id: Any, validTime: Date) =
    db(validTime).assertNoDocument(id)