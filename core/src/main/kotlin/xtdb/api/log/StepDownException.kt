package xtdb.api.log

/**
 * Thrown by a [Log.RecordProcessor] to signal that the processor has been fenced
 * and the subscription should continue polling (to receive rebalance callbacks)
 * rather than terminating.
 */
class StepDownException(cause: Throwable) : Exception("Processor stepped down", cause)
