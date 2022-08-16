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
            [core2.util :as util])
  (:import (core2 ICursor IResultCursor)
           java.lang.AutoCloseable
           java.time.Clock
           (java.util HashMap)
           (java.util.function Function)
           (org.apache.arrow.memory BufferAllocator RootAllocator)))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface PreparedQuery
  (^core2.IResultCursor openCursor [queryArgs])
  (^void close []))

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
        (openCursor [_ {:keys [srcs params current-time default-tz] :as query-opts}]
          (let [{:keys [col-types ->cursor]} (.computeIfAbsent cache
                                                               {:scan-col-types (scan/->scan-col-types srcs scan-cols)
                                                                :param-types (expr/->param-types params)
                                                                :default-tz default-tz}
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
