package xtdb.operator.apply

import xtdb.ICursor
import xtdb.vector.RelationReader

interface DependentCursorFactory {
    fun open(inRel: RelationReader, idx: Int): ICursor<RelationReader>
}