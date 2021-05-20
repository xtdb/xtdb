package crux.api.underware

interface BuilderContext<TYPE> {
    abstract class Companion<TYPE, BUILDER: BuilderContext<TYPE>>(val constructor: () -> BUILDER) {
        fun build(block: BUILDER.() -> Unit) = constructor().also(block).build()
    }

    fun build(): TYPE
}

abstract class SimpleBuilderContext<CLAUSE, TYPE>(
    private val constructor: (List<CLAUSE>) -> TYPE
): BuilderContext<TYPE> {
    private val clauses = mutableListOf<CLAUSE>()

    protected fun add(clause: CLAUSE) {
        clauses.add(clause)
    }

    override fun build() = constructor(clauses)
}

abstract class ComplexBuilderContext<CLAUSE, TYPE>(
    private val constructor: (List<CLAUSE>) -> TYPE
): BuilderContext<TYPE> {
    private val clauses = mutableListOf<CLAUSE>()
    private var hangingClause: CLAUSE? = null

    protected fun add(clause: CLAUSE) {
        lockIn()
        hangingClause = clause
    }

    protected fun replace(clause: CLAUSE) {
        hangingClause = clause
    }

    private fun lockIn() {
        hangingClause?.run(clauses::add)
        hangingClause = null
    }

    override fun build(): TYPE {
        lockIn()
        return constructor(clauses)
    }
}
