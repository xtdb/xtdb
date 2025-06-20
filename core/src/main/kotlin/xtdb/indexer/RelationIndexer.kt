package xtdb.indexer

import xtdb.arrow.RelationReader

interface RelationIndexer {
    fun indexOp(inRelation: RelationReader, queryOpts: Any)
}
