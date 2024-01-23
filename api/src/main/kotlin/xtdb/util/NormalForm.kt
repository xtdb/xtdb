@file:JvmName("NormalForm")

package xtdb.util

import clojure.lang.Keyword
import clojure.lang.Symbol

/**
 * upper case letter preceded by a character that isn't upper-case nor an underscore
 */
private val UPPER_REGEX = Regex("(?<=[^\\p{Lu}_])\\p{Lu}")

/**
 * @suppress
 */
fun normalForm(s: String): String = s
    .replace('-', '_')
    .split('.', '/', '$')
    .joinToString(separator = "$") {
        it
            .replace(UPPER_REGEX) { ch -> "_${ch.value}" }
            .lowercase()
    }

/**
 * @suppress
 */
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

/**
 * @suppress
 */
@Suppress("unused") // Clojure
fun normalForm(k: Keyword): Keyword = Keyword.intern(normalForm(k.sym))

/**
 * @suppress
 */
fun String.kebabToCamelCase(): String {
    val pattern = "-[a-z]".toRegex()
    return replace(pattern) { it.value.last().uppercase() }
}
