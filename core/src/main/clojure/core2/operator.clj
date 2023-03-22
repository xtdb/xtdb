(ns core2.operator
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [core2.error :as err]
            [core2.expression :as expr]
            core2.expression.temporal
            [core2.logical-plan :as lp]
            [core2.metadata :as meta]
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
            [core2.vector.indirect :as iv]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import (clojure.lang IPersistentMap MapEntry)
           (core2 ICursor IResultCursor IResultSet RefCounter)
           core2.metadata.IMetadataManager
           core2.operator.scan.IScanEmitter
           java.lang.AutoCloseable
           (java.time Clock Duration)
           (java.util Iterator)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function Consumer Function)
           (org.apache.arrow.memory BufferAllocator RootAllocator)))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface BoundQuery
  (columnTypes [])
  (^core2.ICursor openCursor [])
  (^void close []
    "optional: if you close this BoundQuery it'll close any closed-over params relation"))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface PreparedQuery
  ;; NOTE we could arguably take the actual params here rather than param-types
  ;; but if we were to make params a VSR this would then make BoundQuery a closeable resource
  ;; ... or at least raise questions about who then owns the params
  (^core2.operator.BoundQuery bind [^core2.watermark.IWatermarkSource watermarkSource, queryOpts]
   "queryOpts :: {:params, :table-args, :basis, :default-tz}"))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IRaQuerySource
  (^core2.operator.PreparedQuery prepareRaQuery [ra-query]))

(defn- ->table-arg-types [table-args]
  (->> (for [[table-key rows] table-args]
         (MapEntry/create
          table-key
          (types/rows->col-types rows)))
       (into {})))

(defn- wrap-cursor ^core2.IResultCursor [^ICursor cursor, ^AutoCloseable wm, ^BufferAllocator al, ^Clock clock, ^RefCounter ref-ctr col-types]
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
      (util/try-close cursor)
      (util/try-close wm)
      (util/try-close al))

    (columnTypes [_] col-types)))

(defn prepare-ra ^core2.operator.PreparedQuery
  ;; this one used from zero-dep tests
  (^core2.operator.PreparedQuery [query] (prepare-ra query {:ref-ctr (RefCounter.)}))

  (^core2.operator.PreparedQuery [query, {:keys [^IScanEmitter scan-emitter, ^IMetadataManager metadata-mgr, ^RefCounter ref-ctr]}]
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
         (bind [_ wm-src {:keys [params table-args basis default-tz]}]
           (assert (or scan-emitter (empty? scan-cols)))

           (let [{:keys [tx after-tx current-time]} basis
                 wm-tx (or tx after-tx)
                 clock (Clock/fixed (or current-time (.instant expr/*clock*))
                                    (or default-tz (.getZone expr/*clock*)))
                 {:keys [col-types ->cursor]} (.computeIfAbsent cache
                                                                {:scan-col-types (when scan-emitter
                                                                                   (with-open [wm (.openWatermark wm-src wm-tx)]
                                                                                     (.scanColTypes scan-emitter wm scan-cols)))
                                                                 :param-types (expr/->param-types params)
                                                                 :table-arg-types (->table-arg-types table-args)
                                                                 :default-tz default-tz
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
                                                 (update :tx (fnil identity (some-> wm .txBasis))))
                                      :params params, :table-args table-args})
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
                                              (.iterator (iv/rel->rows rel))))))
                       (not (and next-values (.hasNext next-values)))))
           (and next-values (.hasNext next-values))))))

  (next [_] (.next next-values))
  (close [_]
    (.close cursor)
    (.close params)))

(defn cursor->result-set ^core2.IResultSet [^IResultCursor cursor, ^AutoCloseable params]
  (CursorResultSet. cursor params nil))

(defn- rows->datalog-rows ^java.lang.Iterable [rows kw->ns-kw]
  (mapv #(update-keys % kw->ns-kw) rows))

(deftype DatalogCursorResultSet [^IResultCursor cursor
                                 ^AutoCloseable params
                                 ^:unsynchronized-mutable ^Iterator next-values
                                 ^IPersistentMap kw->ns-kw]
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
                                              (-> (iv/rel->rows rel)
                                                  (rows->datalog-rows kw->ns-kw)
                                                  (.iterator))))))
                       (not (and next-values (.hasNext next-values)))))
           (and next-values (.hasNext next-values))))))

  (next [_] (.next next-values))
  (close [_]
    (.close cursor)
    (.close params)))

(defn- symbol->ns-keyword [s]
  (apply keyword (str/split (name s) #"__")))

(defn- kw->ns-kw-mapping [column-symbols]
  (into {} (map (juxt keyword symbol->ns-keyword)) column-symbols))

(defn cursor->datalog-result-set ^core2.IResultSet [^IResultCursor cursor, ^AutoCloseable params]
  (let [kw->ns-kw-map (-> (keys (.columnTypes cursor))
                          kw->ns-kw-mapping)]
    (DatalogCursorResultSet. cursor params nil kw->ns-kw-map)))
