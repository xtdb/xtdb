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

/**
 * upper case letter preceded by a character that isn't upper-case nor an underscore
 */
private val UPPER_REGEX = Regex("(?<=[^\\p{Lu}_])\\p{Lu}")

fun normalForm(s: String): String = s
    .replace('-', '_')
    .split('.', '/', '$')
    .joinToString(separator = "$") {
        it
            .replace(UPPER_REGEX) { ch -> "_${ch.value}" }
            .lowercase()
    }

fun normalForm(sym: Symbol): Symbol =
    if (sym.namespace != null) {
        Symbol.intern(
            String.format(
                "%s$%s",
                normalForm(sym.namespace),
                normalForm(sym.name)
            )
        )
    } else {
        Symbol.intern(normalForm(sym.name))
    }

@Suppress("unused") // Clojure
fun normalForm(k: Keyword): Keyword = Keyword.intern(normalForm(k.sym))

fun String.kebabToCamelCase(): String {
    val pattern = "-[a-z]".toRegex()
    return replace(pattern) { it.value.last().uppercase() }
}
