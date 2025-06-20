package xtdb.operator.apply

import xtdb.ICursor
import xtdb.arrow.RelationReader

interface DependentCursorFactory {
    fun open(inRel: RelationReader, idx: Int): ICursor<RelationReader>
}