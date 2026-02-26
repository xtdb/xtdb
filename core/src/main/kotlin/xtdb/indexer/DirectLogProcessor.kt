package xtdb.indexer

import xtdb.compactor.Compactor

data class DirectLogProcessor(
    val replicaLogProcessor: ReplicaLogProcessor,
    val compactor: Compactor.ForDatabase,
    val txSource: Indexer.TxSource?
) : LogProcessor by replicaLogProcessor
