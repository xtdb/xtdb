package crux.api.underware

interface BuilderContext<T> {
    fun build(): T
}