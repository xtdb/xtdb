@file:JvmName("NormalForm")

package xtdb.util

import clojure.lang.Keyword
import clojure.lang.Symbol

@Suppress("unused")
fun snakeCase(s: String): String {
    val i = s.lastIndexOf('$')

    return if (i < 0) {
        s.lowercase()
            .replace("$", "/")
            .replace("-", "_")
    } else {
        String.format(
            "%s/%s",
            s.substring(0, i).replace("$", ".").replace("-", "_"),
            s.substring(i + 1).replace("-", "_")
        )
    }
}

private fun normalise(s: String) =
    s.lowercase()
        .replace('.', '$')
        .replace('-', '_')

@Suppress("unused")
fun normalForm(s: String): String {
    val i = s.lastIndexOf('/')
    return if (i < 0) {
        normalise(s)
    } else {
        String.format("%s$%s", normalise(s.substring(0, i)), normalise(s.substring(i + 1)))
    }
}

@Suppress("MemberVisibilityCanBePrivate") // Clojure
fun normalForm(sym: Symbol): Symbol =
    if (sym.namespace != null) {
        Symbol.intern(
            String.format(
                "%s$%s",
                normalise(sym.namespace),
                normalise(sym.name)
            )
        )
    } else {
        Symbol.intern(normalise(sym.name))
    }

@Suppress("unused") // Clojure
fun normalForm(k: Keyword): Keyword = Keyword.intern(normalForm(k.sym))

fun String.kebabToCamelCase(): String {
    val pattern = "-[a-z]".toRegex()
    return replace(pattern) { it.value.last().uppercase() }
}