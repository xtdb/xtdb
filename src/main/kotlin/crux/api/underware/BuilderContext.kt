package crux.api.underware

interface BuilderContext<T> {
    fun build(): T
}

abstract class BuilderContextCompanion<T, B: BuilderContext<T>>(val constructor: () -> B) {
    fun build(block: B.() -> Unit) = constructor().also(block).build()
}
