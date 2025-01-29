package xtdb.api.log

class IngestionStoppedException(cause: Throwable) :
    IllegalStateException("Ingestion stopped: ${cause.message}", cause)