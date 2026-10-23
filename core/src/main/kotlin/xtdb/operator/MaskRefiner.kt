package xtdb.operator

import org.apache.arrow.memory.BufferAllocator
import xtdb.arrow.MaskedRelation
import xtdb.arrow.VectorReader

/** DART (#6285 avenue B): a predicate that clears bits of a [MaskedRelation]'s mask rather than producing an `int[]`. */
interface MaskRefiner {
    /** @return null if this predicate can't be evaluated against [rel]'s direct vectors alone */
    fun refine(al: BufferAllocator, rel: MaskedRelation, schema: Map<String, Any>, args: xtdb.arrow.RelationReader): MaskedRelation?
}

/** DART: projection evaluated over the set bits of a [MaskedRelation], output dense (one row per set bit). */
interface MaskedProjection {
    /** @return null if this projection can't be evaluated against [rel]'s direct vectors alone */
    fun projectMasked(al: BufferAllocator, rel: MaskedRelation, schema: Map<String, Any>, args: xtdb.arrow.RelationReader): VectorReader?
}
