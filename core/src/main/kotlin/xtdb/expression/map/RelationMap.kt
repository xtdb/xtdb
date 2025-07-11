package xtdb.expression.map

import org.apache.arrow.vector.types.pojo.Field
import xtdb.arrow.RelationReader

interface RelationMap {
    fun buildFields(): Map<String, Field>
    fun buildKeyColumnNames(): List<String>
    fun probeFields(): Map<String, Field>
    fun probeKeyColumnNames(): List<String>
    fun buildFromRelation(inRelation: RelationReader): RelationMapBuilder
    fun probeFromRelation(inRelation: RelationReader): RelationMapProber
    fun getBuiltRelation(): RelationReader
}