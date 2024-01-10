(ns xtdb.query
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
            xtdb.operator.unnest
            [xtdb.sql :as sql]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]
            [xtdb.xtql :as xtql]
            [xtdb.xtql.edn :as xtql.edn])
  (:import clojure.lang.MapEntry
           java.lang.AutoCloseable
           (java.time Clock Duration)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function Function)
           [java.util.stream Stream StreamSupport]
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (xtdb ICursor IResultCursor)
           xtdb.metadata.IMetadataManager
           xtdb.operator.scan.IScanEmitter
           xtdb.api.query.Query
           xtdb.util.RefCounter))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface BoundQuery
  (columnFields [])
  (^xtdb.ICursor openCursor [])
  (^void close []
   "optional: if you close this BoundQuery it'll close any closed-over params relation"))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface PreparedQuery
  ;; NOTE we could arguably take the actual params here rather than param-fields
  ;; but if we were to make params a VSR this would then make BoundQuery a closeable resource
  ;; ... or at least raise questions about who then owns the params
  (^xtdb.query.BoundQuery bind [^xtdb.watermark.IWatermarkSource watermarkSource, queryOpts]
   "queryOpts :: {:params, :table-args, :basis, :default-tz}"))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IRaQuerySource
  (^xtdb.query.PreparedQuery prepareRaQuery [ra-query]))

(defn- wrap-cursor ^xtdb.IResultCursor [^ICursor cursor, ^AutoCloseable wm, ^BufferAllocator al,
                                        ^Clock clock, ^RefCounter ref-ctr fields]
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

    (columnFields [_] fields)))

(defn prepare-ra ^xtdb.query.PreparedQuery
  ;; this one used from zero-dep tests
  (^xtdb.query.PreparedQuery [query] (prepare-ra query {:ref-ctr (RefCounter.)}))

  (^xtdb.query.PreparedQuery [query, {:keys [^IScanEmitter scan-emitter, ^BufferAllocator allocator, ^IMetadataManager metadata-mgr, ^RefCounter ref-ctr]}]
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
         (bind [_ wm-src {:keys [params basis after-tx default-tz default-all-valid-time?]}]
           (assert (or scan-emitter (empty? scan-cols)))

           (let [{:keys [at-tx current-time]} basis
                 current-time (or current-time (.instant expr/*clock*))
                 default-tz (or default-tz (.getZone expr/*clock*))
                 wm-tx (or at-tx after-tx)
                 clock (Clock/fixed current-time default-tz)
                 {:keys [fields ->cursor]} (.computeIfAbsent cache
                                                             {:scan-fields (when (and (seq scan-cols) scan-emitter)
                                                                             (with-open [wm (.openWatermark wm-src wm-tx)]
                                                                               (.scanFields scan-emitter wm scan-cols)))
                                                              :param-fields (expr/->param-fields params)
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
               (columnFields [_] fields)

               (openCursor [_]
                 (.acquire ref-ctr)
                 (let [^BufferAllocator allocator
                       (if allocator
                         (util/->child-allocator allocator "BoundQuery/openCursor")
                         (RootAllocator.))
                       wm (some-> wm-src (.openWatermark wm-tx))]
                   (try
                     (binding [expr/*clock* clock]
                       (-> (->cursor {:allocator allocator, :watermark wm
                                      :clock clock,
                                      :basis (-> basis
                                                 (update :at-tx (fnil identity (some-> wm .txBasis)))
                                                 (assoc :current-time current-time))
                                      :params params, :default-all-valid-time? default-all-valid-time?})
                           (wrap-cursor wm allocator clock ref-ctr fields)))

                     (catch Throwable t
                       (.release ref-ctr)
                       (util/try-close wm)
                       (util/try-close allocator)
                       (throw t)))))

               AutoCloseable
               (close [_] (util/try-close params))))))))))

(defmethod ig/prep-key ::ra-query-source [_ opts]
  (merge opts
         {:allocator (ig/ref :xtdb/allocator)
          :scan-emitter (ig/ref ::scan/scan-emitter)
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

(defmethod ig/halt-key! ::ra-query-source [_ ^AutoCloseable ra-query-source]
  (.close ra-query-source))

(defn compile-query [query query-opts table-info]
  (cond
    (string? query) (sql/compile-query query (-> query-opts (assoc :table-info table-info)))

    (seq? query) (xtql/compile-query (xtql.edn/parse-query query) table-info)

    (instance? Query query) (xtql/compile-query query table-info)

    :else (throw (err/illegal-arg :unknown-query-type {:query query, :type (type query)}))))

(defn- param-sym [v]
  (-> (symbol (str "?" v))
      util/symbol->normal-form-symbol
      (with-meta {:param? true})))

(defn open-args [^BufferAllocator allocator, args]
  (vw/open-params allocator
                  (->> args
                       (into {} (map (fn [[k v]]
                                       (MapEntry/create (param-sym (str (symbol k))) v)))))))

(defn open-query ^java.util.stream.Stream [allocator ^IRaQuerySource ra-src, wm-src, plan, query-opts]
  (let [{:keys [args key-fn]} query-opts
        key-fn (util/parse-key-fn key-fn)]
    (util/with-close-on-catch [args (open-args allocator args)
                               cursor (-> (.prepareRaQuery ra-src plan)
                                          (.bind wm-src (-> query-opts
                                                            (assoc :params args, :key-fn key-fn)))
                                          (.openCursor))]
      (-> (StreamSupport/stream cursor false)
          ^Stream (.onClose (fn []
                              (util/close cursor)
                              (util/close args)))
          (.flatMap (reify Function
                      (apply [_ rel]
                        (.stream (vr/rel->rows rel key-fn)))))))))
