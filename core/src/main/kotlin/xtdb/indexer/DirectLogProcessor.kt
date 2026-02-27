package xtdb.indexer

data class DirectLogProcessor(
    val replicaLogProcessor: ReplicaLogProcessor,
) : LogProcessor by replicaLogProcessor
