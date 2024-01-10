package xtdb.api.query

import clojure.lang.Keyword
import xtdb.util.normalForm
import xtdb.util.snakeCase

fun interface IKeyFn<V> {
    fun denormalize(key: String): V

    companion object {
        private fun clojureFormString(s: String) =
            s.replace('_', '-')
                .replace('$', '.')

        @JvmStatic
        @JvmName("keyword")
        fun keyword(f: IKeyFn<String>) = IKeyFn { k -> Keyword.intern(f.denormalize(k)) }

        @JvmStatic
        @JvmName("cached")
        fun <V : Any> cached(f: IKeyFn<V>): IKeyFn<V> {
            val cache = HashMap<String, V>()
            return IKeyFn { k: String -> cache.computeIfAbsent(k) { key -> f.denormalize(key) } }
        }

        @JvmField
        val CLOJURE: IKeyFn<String> = IKeyFn { s ->
            val i = s.lastIndexOf('$')
            if (i < 0) {
                clojureFormString(s)
            } else {
                String.format("%s/%s", clojureFormString(s.substring(0, i)), clojureFormString(s.substring(i + 1)))
            }
        }

        // TODO the inner hyphen to underscore is not strictly necessary on the way out
        @JvmField
        val SQL: IKeyFn<String> = IKeyFn { s -> normalForm(s) }

        // TODO the inner hyphen to underscore is not strictly necessary on the way out
        @JvmField
        val SNAKE_CASE: IKeyFn<String> = IKeyFn { s -> snakeCase(s) }
    }
}
