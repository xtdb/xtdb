package xtdb.api.log

class IngestionStoppedException(val msgId: MessageId, cause: Throwable) :
    IllegalStateException("Ingestion stopped: ${cause.message}", cause)