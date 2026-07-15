package xtdb.operator.apply

import xtdb.api.ICursor
import xtdb.arrow.RelationReader

interface DependentCursorFactory {
    fun open(inRel: RelationReader, idx: Int): ICursor
}