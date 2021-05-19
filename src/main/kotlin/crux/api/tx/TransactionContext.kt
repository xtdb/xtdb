package crux.api.tx

import crux.api.CruxDocument
import crux.api.ICruxIngestAPI
import java.util.*

class TransactionContext private constructor() {
    companion object {
        fun build(block: TransactionContext.() -> Unit): Transaction =
            TransactionContext().also(block).build()
    }

    private val builder = Transaction.builder()

    data class DocumentToPut(val document: CruxDocument)
    data class DocumentToPutWithValidTime(val document: CruxDocument, val validTime: Date)
    data class IdToDelete(val id: Any)
    data class IdToDeleteWithValidTime(val id: Any, val validTime: Date)
    data class DocumentToMatch(val document: CruxDocument)
    data class IdToNotMatch(val id: Any)

    private var hangingOperation: TransactionOperation? = null

    operator fun CruxDocument.unaryPlus() =
        DocumentToPut(this).also {
            lockIn()
            hangingOperation = PutOperation.create(this)
        }

    infix fun DocumentToPut.from(validTime: Date) =
        DocumentToPutWithValidTime(document, validTime).also {
            hangingOperation = PutOperation.create(document, validTime)
        }

    infix fun DocumentToPutWithValidTime.to(endValidTime: Date) {
        hangingOperation = PutOperation.create(document, validTime, endValidTime)
    }

    operator fun Any.unaryMinus() =
        IdToDelete(this).also {
            lockIn()
            hangingOperation = DeleteOperation.create(this)
        }

    infix fun IdToDelete.from(validTime: Date) =
        IdToDeleteWithValidTime(id, validTime).also {
            hangingOperation = DeleteOperation.create(id, validTime)
        }

    infix fun IdToDeleteWithValidTime.to(endValidTime: Date) {
        hangingOperation = DeleteOperation.create(id, validTime, endValidTime)
    }

    fun evict(id: Any) {
        lockIn()
        hangingOperation = EvictOperation.create(id)
    }

    fun match(document: CruxDocument) =
        DocumentToMatch(document).also {
            lockIn()
            hangingOperation = MatchOperation.create(document)
        }

    infix fun DocumentToMatch.at(validTime: Date) {
        hangingOperation = MatchOperation.create(document, validTime)
    }

    fun notExists(id: Any) =
        IdToNotMatch(id).also {
            lockIn()
            hangingOperation = MatchOperation.create(id)
        }

    infix fun IdToNotMatch.at(validTime: Date) {
        hangingOperation = MatchOperation.create(id, validTime)
    }

    private fun clear() {
        hangingOperation = null
    }

    private fun lockIn() {
        hangingOperation?.let(builder::add)
        clear()
    }

    private fun build(): Transaction {
        lockIn()
        return builder.build()
    }
}

fun ICruxIngestAPI.submitTx(block: TransactionContext.() -> Unit) =
    submitTx(TransactionContext.build(block))