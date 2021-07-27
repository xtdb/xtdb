package crux.api.tx

import crux.api.CruxDocument
import crux.api.query.domain.CruxDocumentSerde
import crux.api.underware.BuilderContext
import java.util.*

class TransactionContext private constructor(): BuilderContext<Transaction> {
    companion object: BuilderContext.Companion<Transaction, TransactionContext>(::TransactionContext)

    private val builder = Transaction.builder()

    /*
     * These are used as halfway houses for infix chains
     */
    data class DocWithValidTime(val document: CruxDocument, val validTime: Date)
    data class DocWithValidTimes(val document: CruxDocument, val validTime: Date, val endValidTime: Date)
    data class DocWithEndValidTime(val document: CruxDocument, val endValidTime: Date)
    data class IdWithValidTime(val id: Any, val validTime: Date)
    data class IdWithValidTimes(val id: Any, val validTime: Date, val endValidTime: Date)
    data class IdWithEndValidTime(val id: Any, val endValidTime: Date)
    data class DocAtValidTime(val document: CruxDocument, val validTime: Date)
    data class IdAtValidTime(val id: Any, val validTime: Date)

    private fun add(transactionOperation: TransactionOperation) {
        builder.add(transactionOperation)
    }

    operator fun TransactionOperation.unaryPlus() = add(this)

    infix fun CruxDocument.from(validTime: Date) = DocWithValidTime(this, validTime)
    infix fun DocWithValidTime.until(endValidTime: Date) = DocWithValidTimes(document, validTime, endValidTime)
    infix fun Any.from(validTime: Date) = IdWithValidTime(this, validTime)
    infix fun IdWithValidTime.until(endValidTime: Date) = IdWithValidTimes(id, validTime, endValidTime)
    infix fun CruxDocument.at(validTime: Date) = DocAtValidTime(this, validTime)
    infix fun Any.at(validTime: Date) = IdAtValidTime(this, validTime)

    infix fun <T> T.by(serde: CruxDocumentSerde<T>) = serde.toDocument(this)

    fun put(document: CruxDocument) = +PutOperation.create(document)
    fun put(data: DocWithValidTime) = +data.run{PutOperation.create(document, validTime)}
    fun put(data: DocWithValidTimes) = +data.run{PutOperation.create(document, validTime, endValidTime)}

    fun delete(id: Any) = +DeleteOperation.create(id)
    fun delete(data: IdWithValidTime) = +data.run{DeleteOperation.create(id, validTime)}
    fun delete(data: IdWithValidTimes) = +data.run{DeleteOperation.create(id, validTime, endValidTime)}

    fun match(document: CruxDocument) = +MatchOperation.create(document)
    fun match(data: DocAtValidTime) = +data.run{MatchOperation.create(document, validTime)}

    fun notExists(id: Any) = +MatchOperation.create(id)
    fun notExists(data: IdAtValidTime) = +data.run{MatchOperation.create(id, validTime)}

    fun evict(id: Any) = +EvictOperation.create(id)

    inner class FromValidTimeContext(private val validTime: Date) {

        infix fun CruxDocument.until(endValidTime: Date) = DocWithEndValidTime(this, endValidTime)
        infix fun Any.until(endValidTime: Date) = IdWithEndValidTime(this, endValidTime)

        fun put(document: CruxDocument) = +PutOperation.create(document, validTime)
        fun put(data: DocWithEndValidTime) = +data.run{PutOperation.create(document, validTime, endValidTime)}

        fun delete(id: Any) = +DeleteOperation.create(id, validTime)
        fun delete(data: IdWithEndValidTime) = +data.run{DeleteOperation.create(id, validTime, endValidTime)}
    }

    fun from(validTime: Date, block: FromValidTimeContext.() -> Unit) = FromValidTimeContext(validTime).apply(block)

    inner class BetweenTimesContext(private val validTime: Date, private val endValidTime: Date) {
        fun put(document: CruxDocument) = +PutOperation.create(document, validTime, endValidTime)
        fun delete(id: Any) = +DeleteOperation.create(id, validTime, endValidTime)
    }

    fun between(validTime: Date, endValidTime: Date, block: BetweenTimesContext.() -> Unit) =
        BetweenTimesContext(validTime, endValidTime).apply(block)

    override fun build(): Transaction = builder.build()
}
