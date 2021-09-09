package xtdb.api.query.domain

import xtdb.api.XtdbDocument

interface XtdbDocumentSerde<T> {
    fun toDocument(obj: T): XtdbDocument
    fun toObject(document: XtdbDocument): T
}