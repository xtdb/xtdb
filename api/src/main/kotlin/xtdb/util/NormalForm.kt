@file:JvmName("NormalForm")

package xtdb.util

import clojure.lang.Keyword
import clojure.lang.Symbol
import com.github.benmanes.caffeine.cache.Caffeine

internal fun normalForm0(s: String): String = s
    .replace('-', '_')
    .replace(Regex("^xt/"), "_")
    .split('.', '/', '$')
    .joinToString(separator = "$")
    .lowercase()

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
    Symbol.intern(
        when (sym.namespace) {
            null -> normalForm(sym.name)
            "xt" -> "_${normalForm(sym.name)}"
            else -> "${normalForm(sym.namespace)}$${normalForm(sym.name)}"
        }
    )

/**
 * @suppress
 */
@Suppress("unused") // Clojure
fun normalForm(k: Keyword): Keyword = Keyword.intern(normalForm(k.sym))

private val String.normalTableSegment get() = lowercase().replace('-', '_')

fun Keyword.normalTableName(): Symbol =
    Symbol.intern(namespace?.normalTableSegment ?: "public", name.normalTableSegment)

/**
 * @suppress
 */
internal fun String.kebabToCamelCase(): String {
    val pattern = "-[a-z]".toRegex()
    return replace(pattern) { it.value.last().uppercase() }
}
