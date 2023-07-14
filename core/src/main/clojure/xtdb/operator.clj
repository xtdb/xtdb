(ns xtdb.operator
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            xtdb.expression.temporal
            [xtdb.logical-plan :as lp]
            [xtdb.metadata :as meta]
            xtdb.operator.apply
            xtdb.operator.arrow
            xtdb.operator.csv
            xtdb.operator.group-by
            xtdb.operator.join
            xtdb.operator.order-by
            xtdb.operator.project
            xtdb.operator.rename
            [xtdb.operator.scan :as scan]
            xtdb.operator.select
            xtdb.operator.set
            xtdb.operator.table
            xtdb.operator.top
            xtdb.operator.unwind
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang MapEntry)
           java.lang.AutoCloseable
           (java.time Clock Duration)
           (java.util Iterator)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function Consumer Function)
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (xtdb ICursor IResultCursor IResultSet RefCounter)
           xtdb.metadata.IMetadataManager
           xtdb.operator.scan.IScanEmitter))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface BoundQuery
  (columnTypes [])
  (^xtdb.ICursor openCursor [])
  (^void close []
    "optional: if you close this BoundQuery it'll close any closed-over params relation"))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface PreparedQuery
  ;; NOTE we could arguably take the actual params here rather than param-types
  ;; but if we were to make params a VSR this would then make BoundQuery a closeable resource
  ;; ... or at least raise questions about who then owns the params
  (^xtdb.operator.BoundQuery bind [^xtdb.watermark.IWatermarkSource watermarkSource, queryOpts]
   "queryOpts :: {:params, :table-args, :basis, :default-tz}"))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IRaQuerySource
  (^xtdb.operator.PreparedQuery prepareRaQuery [ra-query]))

(defn- ->table-arg-types [table-args]
  (->> (for [[table-key rows] table-args]
         (MapEntry/create
          table-key
          (vw/rows->col-types rows)))
       (into {})))

(defn- wrap-cursor ^xtdb.IResultCursor [^ICursor cursor, ^AutoCloseable wm, ^BufferAllocator al, ^Clock clock, ^RefCounter ref-ctr col-types]
  (reify IResultCursor
    (tryAdvance [_ c]
      (when (.isClosing ref-ctr)
        (throw (InterruptedException.)))

      (binding [expr/*clock* clock]
        (.tryAdvance cursor c)))

    (characteristics [_] (.characteristics cursor))
    (estimateSize [_] (.estimateSize cursor))
    (getComparator [_] (.getComparator cursor))
    (getExactSizeIfKnown [_] (.getExactSizeIfKnown cursor))
    (hasCharacteristics [_ c] (.hasCharacteristics cursor c))
    (trySplit [_] (.trySplit cursor))

    (close [_]
      (.release ref-ctr)
      (util/close cursor)
      (util/close wm)
      (util/close al))

    (columnTypes [_] col-types)))

(defn prepare-ra ^xtdb.operator.PreparedQuery
  ;; this one used from zero-dep tests
  (^xtdb.operator.PreparedQuery [query] (prepare-ra query {:ref-ctr (RefCounter.)}))

  (^xtdb.operator.PreparedQuery [query, {:keys [^IScanEmitter scan-emitter, ^IMetadataManager metadata-mgr, ^RefCounter ref-ctr]}]
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
         (bind [_ wm-src {:keys [params table-args basis default-tz default-all-valid-time?]}]
           (assert (or scan-emitter (empty? scan-cols)))

           (let [{:keys [tx after-tx current-time]} basis
                 current-time (or current-time (.instant expr/*clock*))
                 default-tz (or default-tz (.getZone expr/*clock*))
                 wm-tx (or tx after-tx)
                 clock (Clock/fixed current-time default-tz)
                 {:keys [col-types ->cursor]} (.computeIfAbsent cache
                                                                {:scan-col-types (when scan-emitter
                                                                                   (with-open [wm (.openWatermark wm-src wm-tx)]
                                                                                     (.scanColTypes scan-emitter wm scan-cols)))
                                                                 :param-types (expr/->param-types params)
                                                                 :table-arg-types (->table-arg-types table-args)
                                                                 :default-tz default-tz
                                                                 :default-all-valid-time? default-all-valid-time?
                                                                 :last-known-chunk (when metadata-mgr
                                                                                     (.lastEntry (.chunksMetadata metadata-mgr)))}
                                                                (reify Function
                                                                  (apply [_ emit-opts]
                                                                    (binding [expr/*clock* clock]
                                                                      (lp/emit-expr conformed-query (assoc emit-opts :scan-emitter scan-emitter))))))]
             (reify
               BoundQuery
               (columnTypes [_] col-types)

               (openCursor [_]
                 (.acquire ref-ctr)
                 (let [allocator (RootAllocator.)
                       wm (some-> wm-src (.openWatermark wm-tx))]
                   (try
                     (binding [expr/*clock* clock]
                       (-> (->cursor {:allocator allocator, :watermark wm
                                      :clock clock,
                                      :basis (-> basis
                                                 (dissoc :after-tx)
                                                 (update :tx (fnil identity (some-> wm .txBasis)))
                                                 (assoc :current-time current-time))
                                      :params params, :table-args table-args :default-all-valid-time? default-all-valid-time?})
                           (wrap-cursor allocator wm clock ref-ctr col-types)))

                     (catch Throwable t
                       (.release ref-ctr)
                       (util/try-close wm)
                       (util/try-close allocator)
                       (throw t)))))

               AutoCloseable
               (close [_] (util/try-close params))))))))))

(defmethod ig/prep-key ::ra-query-source [_ opts]
  (merge opts
         {:scan-emitter (ig/ref ::scan/scan-emitter)
          :metadata-mgr (ig/ref ::meta/metadata-manager)}))

(defmethod ig/init-key ::ra-query-source [_ deps]
  (let [cache (ConcurrentHashMap.)
        ref-ctr (RefCounter.)
        deps (-> deps (assoc :ref-ctr ref-ctr))]
    (reify
      IRaQuerySource
      (prepareRaQuery [_ query]
        (.computeIfAbsent cache query
                          (reify Function
                            (apply [_ _]
                              (prepare-ra query deps)))))

      AutoCloseable
      (close [_]
        (when-not (.tryClose ref-ctr (Duration/ofMinutes 1))
          (log/warn "Failed to shut down after 60s due to outstanding queries"))))))

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
                                              (.iterator (vr/rel->rows rel))))))
                       (not (and next-values (.hasNext next-values)))))
           (and next-values (.hasNext next-values))))))

  (next [_] (.next next-values))
  (close [_]
    (.close cursor)
    (.close params)))

(defn cursor->result-set ^xtdb.IResultSet [^IResultCursor cursor, ^AutoCloseable params]
  (CursorResultSet. cursor params nil))
