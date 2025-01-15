package xtdb.indexer

import xtdb.vector.RelationReader

interface RelationIndexer {
    fun indexOp(inRelation: RelationReader, queryOpts: Any)
}