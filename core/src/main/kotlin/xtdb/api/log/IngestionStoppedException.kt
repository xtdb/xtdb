package xtdb.api.log

import xtdb.types.MessageId

class IngestionStoppedException(val msgId: MessageId?, cause: Throwable) :
    IllegalStateException("Ingestion stopped (msgId: $msgId): ${cause.message}", cause)