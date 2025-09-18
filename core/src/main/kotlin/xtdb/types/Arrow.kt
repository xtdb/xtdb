package xtdb.types

import org.apache.arrow.vector.types.pojo.Field
import xtdb.trie.ColumnName

object Arrow {
    fun Field.withName(name: ColumnName) = Field(name, fieldType, children)
}