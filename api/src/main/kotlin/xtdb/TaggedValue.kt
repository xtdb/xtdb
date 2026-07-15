package xtdb

import clojure.lang.Keyword

/** @suppress */
data class TaggedValue(val tag: Keyword, val value: Any?) {
    override fun toString() = "(Tagged $tag, $value)"
}