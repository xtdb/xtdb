(ns core2.operator
  (:require [clojure.spec.alpha :as s]
            [core2.error :as err]
            [core2.expression :as expr]
            [core2.logical-plan :as lp]
            core2.operator.apply
            core2.operator.arrow
            core2.operator.csv
            core2.operator.group-by
            core2.operator.join
            core2.operator.max-1-row
            core2.operator.order-by
            core2.operator.project
            core2.operator.rename
            [core2.operator.scan :as scan]
            core2.operator.select
            core2.operator.set
            core2.operator.table
            core2.operator.top
            core2.operator.unwind
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import (core2 ICursor IResultCursor IResultSet)
           java.lang.AutoCloseable
           java.time.Clock
           (java.util HashMap Iterator)
           (java.util.function Consumer Function)
           (org.apache.arrow.memory BufferAllocator RootAllocator)))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface PreparedQuery
  (^core2.IResultCursor openCursor [])
  (^core2.IResultCursor openCursor [queryArgs])
  (^core2.IResultCursor openCursor [queryArgs queryOpts])
  (^void close []))

(defn- args->srcs+params [args]
  (if-not (map? args)
    (recur {'$ args})
    (-> (group-by #(if (lp/source-sym? (key %)) :srcs :params) args)
        (update-vals #(into {} %)))))

(deftype ResultCursor [^BufferAllocator allocator, ^ICursor cursor, col-types]
  IResultCursor
  (columnTypes [_] col-types)
  (tryAdvance [_ c] (.tryAdvance cursor c))

  (characteristics [_] (.characteristics cursor))
  (estimateSize [_] (.estimateSize cursor))
  (getComparator [_] (.getComparator cursor))
  (getExactSizeIfKnown [_] (.getExactSizeIfKnown cursor))
  (hasCharacteristics [_ c] (.hasCharacteristics cursor c))
  (trySplit [_] (.trySplit cursor))

  (close [_]
    (.close cursor)
    (.close allocator)))

(defn open-prepared-ra ^core2.operator.PreparedQuery [query]
  (let [conformed-query (s/conform ::lp/logical-plan query)]
    (when (s/invalid? conformed-query)
      (throw (err/illegal-arg :malformed-query
                              {:plan query
                               :explain (s/explain-data ::lp/logical-plan query)})))

    (let [scan-cols (->> (lp/child-exprs conformed-query)
                         (into #{} (comp (filter (comp #{:scan} :op))
                                         (mapcat scan/->scan-cols))))
          cache (HashMap.)]
      (reify PreparedQuery
        (openCursor [this] (.openCursor this {}))
        (openCursor [this query-args] (.openCursor this query-args {}))

        (openCursor [_ args {:keys [current-time default-tz] :as query-opts}]
          (let [{:keys [srcs params]} (args->srcs+params args)
                {:keys [col-types ->cursor]} (.computeIfAbsent cache
                                                               {:scan-col-types (scan/->scan-col-types srcs scan-cols)
                                                                :param-types (expr/->param-types params)}
                                                               (reify Function
                                                                 (apply [_ emit-opts]
                                                                   (lp/emit-expr conformed-query emit-opts))))
                allocator (RootAllocator.)]
            (try
              (let [cursor (->cursor (into query-opts
                                           {:allocator allocator
                                            :srcs srcs, :params params
                                            :clock (Clock/fixed (or current-time (.instant expr/*clock*))
                                                                ;; will later be provided as part of the 'SQL session' (see §6.32)
                                                                (or default-tz (.getZone expr/*clock*)))}))]

                (ResultCursor. allocator cursor col-types))
              (catch Throwable t
                (util/try-close allocator)
                (throw t)))))

        AutoCloseable
        (close [_]
          (.clear cache))))))

(defn open-ra
  ;; TODO duplicated from above. do we want to keep this as an API? we could go for 'either open a PS or get eager results'?
  (^core2.IResultCursor [query] (open-ra query {}))
  (^core2.IResultCursor [query args] (open-ra query args {}))

  (^core2.IResultCursor [query args {:keys [current-time default-tz] :as query-opts}]
   (let [conformed-query (s/conform ::lp/logical-plan query)]
     (when (s/invalid? conformed-query)
       (throw (err/illegal-arg :malformed-query
                               {:plan query
                                :args args
                                :explain (s/explain-data ::lp/logical-plan query)})))

     (let [allocator (RootAllocator.)]
       (try
         (let [{:keys [srcs params]} (args->srcs+params args)
               scan-col-types (->> (lp/child-exprs conformed-query)
                                   (into #{} (comp (filter (comp #{:scan} :op))
                                                   (mapcat scan/->scan-cols)))
                                   (scan/->scan-col-types srcs))

               ;; now that we're taking scan-col-types out, we might be able to cache emit-expr
               {:keys [col-types ->cursor]} (lp/emit-expr conformed-query
                                                          {:scan-col-types scan-col-types
                                                           :param-types (expr/->param-types params)})

               cursor (->cursor (into query-opts
                                      {:allocator allocator
                                       :srcs srcs, :params params

                                       :clock (Clock/fixed (or current-time (.instant expr/*clock*))
                                                           ;; will later be provided as part of the 'SQL session' (see §6.32)
                                                           (or default-tz (.getZone expr/*clock*)))}))]

           (ResultCursor. allocator cursor col-types))
         (catch Throwable t
           (util/try-close allocator)
           (throw t)))))))

(deftype CursorResultSet [^IResultCursor cursor
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
  (close [_] (.close cursor)))

(defn cursor->result-set ^core2.IResultSet [^ICursor cursor]
  (CursorResultSet. cursor nil))
