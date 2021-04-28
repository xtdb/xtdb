(ns core2.operator
  (:require [core2.operator.group-by :as group-by]
            [core2.operator.join :as join]
            [core2.operator.order-by :as order-by]
            [core2.operator.project :as project]
            [core2.operator.rename :as rename]
            [core2.operator.scan :as scan]
            [core2.operator.select :as select]
            [core2.operator.slice :as slice]
            [core2.operator.set :as set-op]
            [core2.operator.table :as table]
            core2.metadata
            core2.temporal
            [core2.system :as sys])
  (:import core2.buffer_pool.IBufferPool
           core2.metadata.IMetadataManager
           core2.temporal.ITemporalManager
           org.apache.arrow.memory.BufferAllocator))

(definterface IOperatorFactory
  (^core2.IChunkCursor scan [^core2.tx.Watermark watermark
                             ^java.util.List #_<String> colNames,
                             metadataPred
                             ^java.util.Map #_#_<String, IVectorPredicate> colPreds
                             ^longs temporalMinRange
                             ^longs temporalMaxRange])

  (^core2.IChunkCursor table [^java.util.List #_<Map> rows])

  (^core2.IChunkCursor select [^core2.IChunkCursor inCursor
                               ^core2.select.IVectorSchemaRootSelector selector])

  (^core2.IChunkCursor project [^core2.IChunkCursor inCursor
                                ^java.util.List #_<ProjectionSpec> projectionSpecs])

  (^core2.IChunkCursor rename [^core2.IChunkCursor inCursor
                               ^java.util.Map #_#_<String, String> renameMap
                               ^String prefix])

  (^core2.IChunkCursor equiJoin [^core2.IChunkCursor leftCursor
                                 ^String leftColName
                                 ^core2.IChunkCursor rightCursor
                                 ^String rightColName])

  (^core2.IChunkCursor semiEquiJoin [^core2.IChunkCursor leftCursor
                                     ^String leftColName
                                     ^core2.IChunkCursor rightCursor
                                     ^String rightColName])

  (^core2.IChunkCursor antiEquiJoin [^core2.IChunkCursor leftCursor
                                     ^String leftColName
                                     ^core2.IChunkCursor rightCursor
                                     ^String rightColName])

  (^core2.IChunkCursor crossJoin [^core2.IChunkCursor leftCursor
                                  ^core2.IChunkCursor rightCursor])

  (^core2.IChunkCursor orderBy [^core2.IChunkCursor inCursor
                                ^java.util.List #_<SortSpec> orderSpecs])

  (^core2.IChunkCursor groupBy [^core2.IChunkCursor inCursor
                                ^java.util.List #_<AggregateSpec> aggregateSpecs])

  (^core2.IChunkCursor slice [^core2.IChunkCursor inCursor, ^Long offset, ^Long limit])

  (^core2.IChunkCursor union [^core2.IChunkCursor leftCursor ^core2.IChunkCursor rightCursor])

  (^core2.IChunkCursor difference [^core2.IChunkCursor leftCursor ^core2.IChunkCursor rightCursor])

  (^core2.IChunkCursor intersection [^core2.IChunkCursor leftCursor ^core2.IChunkCursor rightCursor])

  (^core2.IChunkCursor distinct [^core2.IChunkCursor inCursor])

  (^core2.IChunkCursor fixpoint [^core2.IChunkCursor baseCursor
                                 ^core2.operator.set.IFixpointCursorFactory recursiveCursorFactory
                                 ^boolean isIncremental]))

(defn ->operator-factory {::sys/deps {:allocator :core2/allocator
                                      :metadata-mgr :core2/metadata-manager
                                      :temporal-mgr :core2/temporal-manager
                                      :buffer-pool :core2/buffer-pool}}
  ^core2.operator.IOperatorFactory
  [{:keys [^BufferAllocator allocator
           ^IMetadataManager metadata-mgr
           ^ITemporalManager temporal-mgr
           ^IBufferPool buffer-pool]}]

  (reify IOperatorFactory
    (scan [_ watermark col-names metadata-pred col-preds temporal-min-range temporal-max-range]
      (scan/->scan-cursor allocator metadata-mgr temporal-mgr buffer-pool
                          watermark col-names metadata-pred col-preds temporal-min-range temporal-max-range))

    (table [_ rows]
      (table/->table-cursor allocator rows))

    (select [_ in-cursor selector]
      (select/->select-cursor allocator in-cursor selector))

    (project [_ in-cursor projection-specs]
      (project/->project-cursor allocator in-cursor projection-specs))

    (rename [_ in-cursor rename-map prefix]
      (rename/->rename-cursor allocator in-cursor rename-map prefix))

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
      (slice/->slice-cursor in-cursor offset limit))

    (union [_ left-cursor right-cursor]
      (set-op/->union-cursor left-cursor right-cursor))

    (difference [_ left-cursor right-cursor]
      (set-op/->difference-cursor allocator left-cursor right-cursor))

    (intersection [_ left-cursor right-cursor]
      (set-op/->intersection-cursor allocator left-cursor right-cursor))

    (distinct [_ in-cursor]
      (set-op/->distinct-cursor allocator in-cursor))

    (fixpoint [_ base-cursor recursive-cursor-factory incremental?]
      (set-op/->fixpoint-cursor allocator base-cursor recursive-cursor-factory incremental?))))
