(ns core2.operator
  (:require [core2.operator.scan :as scan]
            [core2.operator.slice :as slice])
  (:import core2.buffer_pool.BufferPool
           core2.metadata.IMetadataManager
           org.apache.arrow.memory.BufferAllocator))

(definterface IOperatorFactory
  (^core2.ICursor scan [^core2.tx.Watermark watermark
                        ^java.util.List #_<String> colNames,
                        metadataPred
                        ^java.util.Map #_#_<String, IVectorPredicate> colPreds])

  (^core2.ICursor select [^core2.ICursor inCursor
                          ^core2.select.IVectorSchemaRootPredicate pred])

  (^core2.ICursor project [^core2.ICursor inCursor
                           ^java.util.List #_#_<Pair<String, ColExpr>> colSpecs])

  (^core2.ICursor rename [^core2.ICursor inCursor
                          ^java.util.Map #_#_<String, String> renameSpecs])

  (^core2.ICursor equiJoin [^core2.ICursor leftCursor
                            ^String leftColName
                            ^core2.ICursor rightCursor
                            ^String rightColName])

  (^core2.ICursor crossJoin [^core2.ICursor leftCursor
                             ^core2.ICursor rightCursor])

  (^core2.ICursor order [^core2.ICursor inCursor
                         ^java.util.List #_#_<Pair<String, Direction>> orderSpec])

  (^core2.ICursor groupBy [^core2.ICursor inCursor
                           ^java.util.List #_#_#_<Pair<String, Or<String, AggregateExpr>>> aggregateSpecs])

  (^core2.ICursor slice [^core2.ICursor inCursor, ^Long offset, ^Long limit]))

(defn ->operator-factory
  ^core2.operator.IOperatorFactory
  [^BufferAllocator allocator
   ^IMetadataManager metadata-mgr
   ^BufferPool buffer-pool]

  (reify IOperatorFactory
    (scan [_ watermark col-names metadata-pred col-preds]
      (scan/->scan-cursor allocator metadata-mgr buffer-pool
                          watermark col-names metadata-pred col-preds))

    (slice [_ in-cursor offset limit]
      (slice/->slice-cursor in-cursor offset limit))))
