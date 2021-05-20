package crux.api.tx

import crux.api.CruxDocument
import crux.api.underware.BuilderContext
import java.util.*

class TransactionContext private constructor(): BuilderContext<Transaction> {
    companion object {
        fun build(block: TransactionContext.() -> Unit): Transaction =
            TransactionContext().also(block).build()
    }

    private val builder = Transaction.builder()

    /*
     * These are used as halfway houses for the first portion of an infix chain
     */
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

    infix fun DocumentToPutWithValidTime.until(endValidTime: Date) {
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

    infix fun IdToDeleteWithValidTime.until(endValidTime: Date) {
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

    inner class FromValidTimeContext(private val validTime: Date) {
        operator fun CruxDocument.unaryPlus() =
            DocumentToPutWithValidTime(this, validTime).also {
                lockIn()
                hangingOperation = PutOperation.create(this, validTime)
            }

        operator fun Any.unaryMinus() =
            IdToDeleteWithValidTime(this, validTime).also {
                lockIn()
                hangingOperation = DeleteOperation.create(this, validTime)
            }
    }

    fun from(validTime: Date, block: FromValidTimeContext.() -> Unit) = FromValidTimeContext(validTime).apply(block)

    inner class BetweenTimesContext(private val validTime: Date, private val endValidTime: Date) {
        operator fun CruxDocument.unaryPlus() {
            lockIn()
            hangingOperation = PutOperation.create(this, validTime, endValidTime)
        }

        operator fun Any.unaryMinus() {
            lockIn()
            hangingOperation = DeleteOperation.create(this, validTime, endValidTime)
        }
    }

    fun between(validTime: Date, endValidTime: Date, block: BetweenTimesContext.() -> Unit) =
        BetweenTimesContext(validTime, endValidTime).apply(block)

    private fun lockIn() {
        hangingOperation?.let(builder::add)
        hangingOperation = null
    }

    override fun build(): Transaction {
        lockIn()
        return builder.build()
    }
}
