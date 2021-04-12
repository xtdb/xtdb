(ns core2.operator
  (:require [core2.operator.group-by :as group-by]
            [core2.operator.join :as join]
            [core2.operator.order-by :as order-by]
            [core2.operator.project :as project]
            [core2.operator.rename :as rename]
            [core2.operator.scan :as scan]
            [core2.operator.select :as select]
            [core2.operator.slice :as slice]
            core2.metadata
            core2.temporal)
  (:import core2.buffer_pool.BufferPool
           core2.metadata.IMetadataManager
           core2.temporal.ITemporalManager
           org.apache.arrow.memory.BufferAllocator))

(definterface IOperatorFactory
  (^core2.ICursor scan [^core2.tx.Watermark watermark
                        ^java.util.List #_<String> colNames,
                        metadataPred
                        ^java.util.Map #_#_<String, IVectorPredicate> colPreds
                        ^longs temporalMinRange
                        ^longs temporalMaxRange])

  (^core2.ICursor select [^core2.ICursor inCursor
                          ^core2.select.IVectorSchemaRootSelector selector])

  (^core2.ICursor project [^core2.ICursor inCursor
                           ^java.util.List #_<ProjectionSpec> projectionSpecs])

  (^core2.ICursor rename [^core2.ICursor inCursor
                          ^java.util.Map #_#_<String, String> renameMap])

  (^core2.ICursor equiJoin [^core2.ICursor leftCursor
                            ^String leftColName
                            ^core2.ICursor rightCursor
                            ^String rightColName])

  (^core2.ICursor semiEquiJoin [^core2.ICursor leftCursor
                                ^String leftColName
                                ^core2.ICursor rightCursor
                                ^String rightColName])

  (^core2.ICursor antiEquiJoin [^core2.ICursor leftCursor
                                ^String leftColName
                                ^core2.ICursor rightCursor
                                ^String rightColName])

  (^core2.ICursor crossJoin [^core2.ICursor leftCursor
                             ^core2.ICursor rightCursor])

  (^core2.ICursor orderBy [^core2.ICursor inCursor
                           ^java.util.List #_<SortSpec> orderSpecs])

  (^core2.ICursor groupBy [^core2.ICursor inCursor
                           ^java.util.List #_<AggregateSpec> aggregateSpecs])

  (^core2.ICursor slice [^core2.ICursor inCursor, ^Long offset, ^Long limit]))

(defn ->operator-factory
  ^core2.operator.IOperatorFactory
  [^BufferAllocator allocator
   ^IMetadataManager metadata-mgr
   ^ITemporalManager temporal-mgr
   ^BufferPool buffer-pool]

  (reify IOperatorFactory
    (scan [_ watermark col-names metadata-pred col-preds temporal-min-range temporal-max-range]
      (scan/->scan-cursor allocator metadata-mgr temporal-mgr buffer-pool
                          watermark col-names metadata-pred col-preds temporal-min-range temporal-max-range))

    (select [_ in-cursor selector]
      (select/->select-cursor allocator in-cursor selector))

    (project [_ in-cursor projection-specs]
      (project/->project-cursor allocator in-cursor projection-specs))

    (rename [_ in-cursor rename-map]
      (rename/->rename-cursor allocator in-cursor rename-map))

    (equiJoin [_ left-cursor left-column-name right-cursor right-column-name]
      (join/->equi-join-cursor allocator left-cursor left-column-name right-cursor right-column-name))

    (semiEquiJoin [_ left-cursor left-column-name right-cursor right-column-name]
      (join/->semi-equi-join-cursor allocator left-cursor left-column-name right-cursor right-column-name))

    (antiEquiJoin [_ left-cursor left-column-name right-cursor right-column-name]
      (join/->anti-equi-join-cursor allocator left-cursor left-column-name right-cursor right-column-name))

    (crossJoin [_ left-cursor right-cursor]
      (join/->cross-join-cursor allocator left-cursor right-cursor))

    (groupBy [_ in-cursor aggregate-specs]
      (group-by/->group-by-cursor allocator in-cursor aggregate-specs))

    (orderBy [_ in-cursor order-specs]
      (order-by/->order-by-cursor allocator in-cursor order-specs))

    (slice [_ in-cursor offset limit]
      (slice/->slice-cursor in-cursor offset limit))))
