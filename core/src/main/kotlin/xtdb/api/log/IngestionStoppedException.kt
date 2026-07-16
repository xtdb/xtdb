package xtdb.api.log

import xtdb.types.MessageId

/** @suppress */
class IngestionStoppedException(val msgId: MessageId?, cause: Throwable) :
    IllegalStateException("Ingestion stopped (msgId: $msgId): ${cause.message}", cause)