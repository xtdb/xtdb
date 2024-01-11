package xtdb.api.query

import clojure.lang.Keyword
import xtdb.util.normalForm
import xtdb.util.snakeCase

fun interface IKeyFn<out V> {

    enum class KeyFn : IKeyFn<Any> {
        CLOJURE_STR {
            private fun clojureFormString(s: String) =
                s.replace('_', '-')
                    .replace('$', '.')

            override fun denormalize(key: String): String {
                val i = key.lastIndexOf('$')
                return if (i < 0) {
                    clojureFormString(key)
                } else {
                    String.format(
                        "%s/%s",
                        clojureFormString(key.substring(0, i)),
                        clojureFormString(key.substring(i + 1))
                    )
                }
            }
        },

        CLOJURE_KW {
            override fun denormalize(key: String): Keyword = Keyword.intern(CLOJURE_STR.denormalize(key) as String)
        },

        SQL_STR {
            override fun denormalize(key: String) = normalForm(key)
        },

        SQL_KW {
            override fun denormalize(key: String): Keyword = Keyword.intern(SQL_STR.denormalize(key) as String)
        },

        SNAKE_CASE_STR {
            override fun denormalize(key: String) = snakeCase(key)
        },

        SNAKE_CASE_KW {
            override fun denormalize(key: String): Keyword = Keyword.intern(SNAKE_CASE_STR.denormalize(key) as String)
        },
    }

    fun denormalize(key: String): V
}
