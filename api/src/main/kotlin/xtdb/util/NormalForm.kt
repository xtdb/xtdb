@file:JvmName("NormalForm")

package xtdb.util

import clojure.lang.Keyword
import clojure.lang.Symbol
import com.github.benmanes.caffeine.cache.Caffeine

/**
 * upper case letter preceded by a character that isn't upper-case nor an underscore
 */
private val UPPER_REGEX = Regex("(?<=[^\\p{Lu}_])\\p{Lu}")

internal fun normalForm0(s: String): String = s
    .replace('-', '_')
    .split('.', '/', '$')
    .joinToString(separator = "$") {
        it
            .replace(UPPER_REGEX) { ch -> "_${ch.value}" }
            .lowercase()
    }

private val NORMAL_FORM_CACHE = Caffeine.newBuilder()
    .maximumSize(1 shl 15)
    .build<String, String>(::normalForm0)

/**
 * @suppress
 */
fun normalForm(s: String): String = NORMAL_FORM_CACHE[s]

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
