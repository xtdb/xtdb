package xtdb

/**
 * Marks a declaration as internal to XTDB: it is not part of the public API contract and may change
 * or be removed without notice. Kotlin callers must opt in with `@OptIn(InternalApi::class)`; Clojure
 * callers, which don't observe Kotlin opt-in, are unaffected.
 */
@RequiresOptIn(
    message = "Internal XTDB API — not part of the public API contract; may change or be removed without notice.",
    level = RequiresOptIn.Level.ERROR,
)
@Retention(AnnotationRetention.BINARY)
@Target(AnnotationTarget.CLASS, AnnotationTarget.PROPERTY, AnnotationTarget.FUNCTION, AnnotationTarget.CONSTRUCTOR)
annotation class InternalApi
