package xtdb.types

import clojure.lang.PersistentHashSet
import org.apache.arrow.vector.types.pojo.Field
import xtdb.trie.ColumnName
import xtdb.util.requiringResolve

object Arrow {
    fun Field.withName(name: ColumnName) = Field(name, fieldType, children)

    fun mergeFields(fields: List<Field>): Field =
        // TODO: migrate over here
        requiringResolve("xtdb.types/merge-fields").applyTo(PersistentHashSet.create(fields).seq()) as Field
}