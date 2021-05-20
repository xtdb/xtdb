package crux.api.underware

interface BuilderContext<T> {
    fun build(): T
}

abstract class SimpleBuilderContext<C, T>(
    private val constructor: (List<C>) -> T
): BuilderContext<T> {
    private val clauses = mutableListOf<C>()

    protected fun add(clause: C) {
        clauses.add(clause)
    }

    override fun build() = constructor(clauses)
}

abstract class ComplexBuilderContext<C, T>(
    private val constructor: (List<C>) -> T
): BuilderContext<T> {
    private val clauses = mutableListOf<C>()
    private var hangingClause: C? = null

    protected fun add(clause: C) {
        lockIn()
        hangingClause = clause
    }

    protected fun replace(clause: C) {
        hangingClause = clause
    }

    private fun lockIn() {
        hangingClause?.run(clauses::add)
        hangingClause = null
    }

    override fun build(): T {
        lockIn()
        return constructor(clauses)
    }
}

abstract class BuilderContextCompanion<T, B: BuilderContext<T>>(val constructor: () -> B) {
    fun build(block: B.() -> Unit) = constructor().also(block).build()
}
