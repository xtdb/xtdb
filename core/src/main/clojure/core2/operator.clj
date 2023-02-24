(ns core2.operator
  (:require [clojure.spec.alpha :as s]
            [core2.error :as err]
            [core2.expression :as expr]
            core2.expression.temporal
            [core2.logical-plan :as lp]
            core2.operator.apply
            core2.operator.arrow
            core2.operator.csv
            core2.operator.group-by
            core2.operator.join
            core2.operator.order-by
            core2.operator.project
            core2.operator.rename
            [core2.operator.scan :as scan]
            core2.operator.select
            core2.operator.set
            core2.operator.table
            core2.operator.top
            core2.operator.unwind
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import clojure.lang.MapEntry
           (core2 ICursor IResultCursor IResultSet)
           core2.operator.scan.ScanSource
           java.lang.AutoCloseable
           java.time.Clock
           (java.util Iterator)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function Consumer Function)
           (org.apache.arrow.memory BufferAllocator RootAllocator)))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface BoundQuery
  (columnTypes [])
  (^core2.ICursor openCursor [])
  (^void close []
    "optional: if you close this BoundQuery it'll close any closed-over params relation"))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface PreparedQuery
  ;; NOTE we could arguably take the actual params here rather than param-types
  ;; but if we were to make params a VSR this would then make BoundQuery a closeable resource
  ;; ... or at least raise questions about who then owns the params
  (^core2.operator.BoundQuery bind [queryOpts]
   "queryOpts :: {:srcs, :params, :current-time, :default-tz}"))

(defn- ->table-arg-types [table-args]
  (->> (for [[table-key rows] table-args]
         (MapEntry/create
          table-key
          (types/rows->col-types rows)))
       (into {})))

(defn- wrap-cursor ^core2.ICursor [^ICursor cursor, ^BufferAllocator al, ^Clock clock]
  (reify ICursor
    (tryAdvance [_ c]
      (binding [expr/*clock* clock]
        (.tryAdvance cursor c)))

    (characteristics [_] (.characteristics cursor))
    (estimateSize [_] (.estimateSize cursor))
    (getComparator [_] (.getComparator cursor))
    (getExactSizeIfKnown [_] (.getExactSizeIfKnown cursor))
    (hasCharacteristics [_ c] (.hasCharacteristics cursor c))
    (trySplit [_] (.trySplit cursor))

    (close [_]
      (util/try-close cursor)
      (util/try-close al))))

(defn prepare-ra ^core2.operator.PreparedQuery [query]
  (let [conformed-query (s/conform ::lp/logical-plan query)]
    (when (s/invalid? conformed-query)
      (throw (err/illegal-arg :malformed-query
                              {:plan query
                               :explain (s/explain-data ::lp/logical-plan query)})))

    (let [scan-cols (->> (lp/child-exprs conformed-query)
                         (into #{} (comp (filter (comp #{:scan} :op))
                                         (mapcat scan/->scan-cols))))
          cache (ConcurrentHashMap.)]
      (reify PreparedQuery
        (bind [_ {:keys [srcs params table-args current-time default-tz]}]
          (let [clock (Clock/fixed (or current-time (.instant expr/*clock*))
                                   (or default-tz (.getZone expr/*clock*)))
                {:keys [col-types ->cursor]} (.computeIfAbsent cache
                                                               {:scan-col-types (scan/->scan-col-types srcs scan-cols)
                                                                :param-types (expr/->param-types params)
                                                                :table-arg-types (->table-arg-types table-args)
                                                                :default-tz default-tz
                                                                :last-known-chunk-by-src (into
                                                                                           {}
                                                                                           (for [[src-key ^ScanSource src] srcs]
                                                                                             (MapEntry/create
                                                                                               src-key
                                                                                               (.lastEntry (.chunksMetadata (.metadataManager src))))))}
                                                               (reify Function
                                                                 (apply [_ emit-opts]
                                                                   (binding [expr/*clock* clock]
                                                                     (lp/emit-expr conformed-query (assoc emit-opts :srcs srcs))))))]
            (reify
              BoundQuery
              (columnTypes [_] col-types)

              (openCursor [_]
                (let [allocator (RootAllocator.)]
                  (try
                    (binding [expr/*clock* clock]
                      (-> (->cursor {:allocator allocator, :clock clock, :srcs srcs, :params params, :table-args table-args})
                          (wrap-cursor allocator clock)))

                    (catch Throwable t
                      (util/try-close allocator)
                      (throw t)))))

              AutoCloseable
              (close [_] (util/try-close params)))))))))

(deftype CursorResultSet [^IResultCursor cursor
                          ^AutoCloseable params
                          ^:unsynchronized-mutable ^Iterator next-values]
  IResultSet
  (columnTypes [_] (.columnTypes cursor))

  (hasNext [res]
    (boolean
     (or (and next-values (.hasNext next-values))
         ;; need to call rel->rows eagerly - the rel may have been reused/closed after
         ;; the tryAdvance returns.
         (do
           (while (and (.tryAdvance cursor
                                    (reify Consumer
                                      (accept [_ rel]
                                        (set! (.-next-values res)
                                              (.iterator (iv/rel->rows rel))))))
                       (not (and next-values (.hasNext next-values)))))
           (and next-values (.hasNext next-values))))))

  (next [_] (.next next-values))
  (close [_]
    (.close cursor)
    (.close params)))

(defn cursor->result-set ^core2.IResultSet [^IResultCursor cursor, ^AutoCloseable params]
   (CursorResultSet. cursor params nil))
