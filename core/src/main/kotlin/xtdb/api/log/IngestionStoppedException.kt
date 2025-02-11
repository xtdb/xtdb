package xtdb.api.log

class IngestionStoppedException(val logOffset: LogOffset, cause: Throwable) :
    IllegalStateException("Ingestion stopped: ${cause.message}", cause)