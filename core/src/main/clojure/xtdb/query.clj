(ns xtdb.query
  (:require [clojure.pprint :as pp]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            xtdb.expression.temporal
            [xtdb.information-schema :as info-schema]
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
           java.util.HashMap
           [java.util.stream Stream StreamSupport]
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (xtdb ICursor IResultCursor)
           (xtdb.api.query IKeyFn Query)
           xtdb.metadata.IMetadataManager
           xtdb.operator.scan.IScanEmitter
           xtdb.util.RefCounter
           xtdb.watermark.IWatermarkSource))

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
  (^xtdb.query.BoundQuery bind [queryOpts]
   "queryOpts :: {:params, :table-args, :basis, :default-tz}"))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IQuerySource
  (^xtdb.query.PreparedQuery prepareRaQuery [ra-query wm-src])
  (^clojure.lang.PersistentVector planQuery [query wm-src query-opts]))

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

(defn- param-sym [v]
  (-> (symbol (str "?" v))
      util/symbol->normal-form-symbol
      (with-meta {:param? true})))

(defn open-args [^BufferAllocator allocator, args]
  (vw/open-params allocator
                  (->> args
                       (into {} (map-indexed (fn [idx v ]
                                               (if (map-entry? v)
                                                 (MapEntry/create (param-sym (str (symbol (key v)))) (val v))
                                                 (MapEntry/create (symbol (str "?_" idx)) v))))))))

(defn prepare-ra ^xtdb.query.PreparedQuery
  ;; this one used from zero-dep tests
  (^xtdb.query.PreparedQuery [query] (prepare-ra query {:ref-ctr (RefCounter.)}))

  (^xtdb.query.PreparedQuery [query, {:keys [^IScanEmitter scan-emitter, ^BufferAllocator allocator, ^IMetadataManager metadata-mgr,
                                             ^RefCounter ref-ctr ^IWatermarkSource wm-src]}]
   (let [conformed-query (s/conform ::lp/logical-plan query)]
     (when (s/invalid? conformed-query)
       (throw (err/illegal-arg :malformed-query
                               {:plan query
                                :explain (s/explain-data ::lp/logical-plan query)})))

     (let [scan-cols (->> (lp/child-exprs conformed-query)
                          (into #{} (comp (filter (comp #{:scan} :op))
                                          (mapcat scan/->scan-cols))))

           _ (assert (or scan-emitter (empty? scan-cols)))

           cache (ConcurrentHashMap.)

           ordered-outer-projection (:named-projection (meta query))]

       (reify PreparedQuery
         (bind [_ {:keys [args params basis default-tz default-all-valid-time?]}]

           (util/with-close-on-catch [args (open-args allocator args)]
             ;;TODO consider making the either/or relationship between params/args explicit, e.g throw error if both are provided
             (let [params (or params args)
                   {:keys [current-time]} basis
                   current-time (or current-time (.instant expr/*clock*))
                   default-tz (or default-tz (.getZone expr/*clock*))
                   clock (Clock/fixed current-time default-tz)

                   {:keys [fields ->cursor]} (.computeIfAbsent cache
                                                               {:scan-fields (when (and (seq scan-cols) scan-emitter)
                                                                               (with-open [wm (.openWatermark wm-src)]
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
                 (columnFields [_]
                   (if ordered-outer-projection
                     (mapv #(hash-map (str %) (get fields %)) ordered-outer-projection)
                     (mapv #(hash-map (key %) (val %)) fields)))
                 (openCursor [_]
                   (.acquire ref-ctr)
                   (let [^BufferAllocator allocator
                         (if allocator
                           (util/->child-allocator allocator "BoundQuery/openCursor")
                           (RootAllocator.))
                         wm (some-> wm-src (.openWatermark))]
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
                 (close [_] (util/try-close params)))))))))))

(defmethod ig/prep-key ::query-source [_ opts]
  (merge opts
         {:allocator (ig/ref :xtdb/allocator)
          :scan-emitter (ig/ref ::scan/scan-emitter)
          :metadata-mgr (ig/ref ::meta/metadata-manager)}))

(defmethod ig/init-key ::query-source [_ deps]
  (let [prepare-cache (ConcurrentHashMap.)
        plan-cache (ConcurrentHashMap.)
        ref-ctr (RefCounter.)
        deps (-> deps (assoc :ref-ctr ref-ctr))]
    (reify
      IQuerySource
      (prepareRaQuery [_ query wm-src]
        (.computeIfAbsent prepare-cache [query wm-src]
                          (reify Function
                            (apply [_ _]
                              (prepare-ra query (assoc deps :wm-src wm-src))))))

      (planQuery [_ query wm-src query-opts]
        (let [table-info (scan/tables-with-cols wm-src (:scan-emitter deps))
              plan-query-opts
              (-> query-opts
                  (select-keys
                   [:default-all-valid-time? :decorrelate? :explain?
                    :instrument-rules? :project-anonymous-columns? :validate-plan?])
                  (update :decorrelate? #(if (nil? %) true false))
                  (assoc :table-info table-info))
              ;;TODO defaults to true in rewrite plan so needs defaulting pre-cache,
              ;;Move all defaulting to this level if/when everyone goes via planQuery
              cache-key (assoc plan-query-opts :query query)]
          (.computeIfAbsent
           plan-cache
           cache-key
           (reify Function
             (apply [_ _]
               (let [plan (cond
                            (string? query) (sql/compile-query query plan-query-opts)

                            (seq? query) (xtql/compile-query (xtql.edn/parse-query query) plan-query-opts)

                            (instance? Query query) (xtql/compile-query query plan-query-opts)

                            :else (throw (err/illegal-arg :unknown-query-type {:query query, :type (type query)})))]
                 (if (:explain? query-opts)
                   [:table [{:plan (with-out-str (pp/pprint plan))}]]
                   plan)))))))

      AutoCloseable
      (close [_]
        (when-not (.tryClose ref-ctr (Duration/ofMinutes 1))
          (log/warn "Failed to shut down after 60s due to outstanding queries"))))))

(defmethod ig/halt-key! ::query-source [_ ^AutoCloseable query-source]
  (.close query-source))

(defn- cache-key-fn [^IKeyFn key-fn]
  (let [cache (HashMap.)]
    (reify IKeyFn
      (denormalize [_ k]
        (.computeIfAbsent cache k
                          (reify Function
                            (apply [_ k]
                              (.denormalize key-fn k))))))))

(defn open-cursor-as-stream ^java.util.stream.Stream [^BoundQuery bound-query {:keys [key-fn]}]
  (let [key-fn (cache-key-fn key-fn)]
    (util/with-close-on-catch [cursor (.openCursor bound-query)]
      (-> (StreamSupport/stream cursor false)
          ^Stream (.onClose (fn []
                              (util/close cursor)
                              (util/close bound-query)))
          (.flatMap (reify Function
                      (apply [_ rel]
                        (.stream (vr/rel->rows rel key-fn)))))))))
