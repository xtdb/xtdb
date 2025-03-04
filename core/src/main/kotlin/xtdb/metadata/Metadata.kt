package xtdb.metadata

import java.util.function.IntPredicate

interface MetadataPredicate {
    fun build(tableMetadata: PageMetadata): IntPredicate
}
