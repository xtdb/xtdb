package crux.api.underware

interface BuilderContext<T> {
    fun build(): T
}

abstract class SimpleBuilderContext<C, T>(
    val constructor: (List<C>) -> T
): BuilderContext<T> {
    private val clauses = mutableListOf<C>()

    protected fun add(clause: C) {
        clauses.add(clause)
    }

    override fun build() = constructor(clauses)
}

abstract class BuilderContextCompanion<T, B: BuilderContext<T>>(val constructor: () -> B) {
    fun build(block: B.() -> Unit) = constructor().also(block).build()
}
