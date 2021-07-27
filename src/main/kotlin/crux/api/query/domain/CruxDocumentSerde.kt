package crux.api.query.domain

import crux.api.CruxDocument

interface CruxDocumentSerde<T> {
    fun toDocument(obj: T): CruxDocument
    fun toObject(document: CruxDocument): T
}