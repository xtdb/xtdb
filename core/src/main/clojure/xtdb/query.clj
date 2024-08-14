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
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]
            [xtdb.xtql :as xtql]
            [xtdb.xtql.edn :as xtql.edn])
  (:import clojure.lang.MapEntry
           (com.github.benmanes.caffeine.cache Caffeine Cache)
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
  (paramFields [])
  (columnFields [])
  (^xtdb.query.BoundQuery bind [queryOpts]
   "queryOpts :: {:params, :table-args, :basis, :default-tz}"))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface IQuerySource
  (^xtdb.query.PreparedQuery prepareRaQuery [ra-query wm-src query-opts])
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
      util/symbol->normal-form-symbol))

(defn mapify-params [params]
  (->> params
       (map-indexed (fn [idx v]
                      (if (map-entry? v)
                        {(param-sym (str (symbol (key v)))) (val v)}
                        {(symbol (str "?_" idx)) v})))))

(defn open-args [^BufferAllocator allocator, args]
  (vw/open-params allocator (into {} (mapify-params args))))

(defn emit-expr [^ConcurrentHashMap cache {:keys [^IScanEmitter scan-emitter, ^IMetadataManager metadata-mgr, ^IWatermarkSource wm-src]}
                 conformed-query scan-cols default-tz param-fields]
  (.computeIfAbsent cache
                    {:scan-fields (when (and (seq scan-cols) scan-emitter)
                                    (with-open [wm (.openWatermark wm-src)]
                                      (.scanFields scan-emitter wm scan-cols)))
                     :default-tz default-tz
                     :last-known-chunk (when metadata-mgr
                                         (.lastEntry (.chunksMetadata metadata-mgr)))
                     :param-fields param-fields}
                    (reify Function
                      (apply [_ emit-opts]
                        (binding [expr/*clock* (Clock/fixed (.instant expr/*clock*) default-tz)]
                          ;; only the tz in the clock is relevant at expr compile time
                          (lp/emit-expr conformed-query (assoc emit-opts :scan-emitter scan-emitter)))))))

(defn ->column-fields [ordered-outer-projection fields]
  (if ordered-outer-projection
    (mapv #(hash-map (str %) (get fields %)) ordered-outer-projection)
    (mapv #(hash-map (key %) (val %)) fields)))

(defn prepare-ra ^xtdb.query.PreparedQuery
  ;; this one used from zero-dep tests
  (^xtdb.query.PreparedQuery [query] (prepare-ra query {:ref-ctr (RefCounter.)} {}))

  (^xtdb.query.PreparedQuery [query, {:keys [^IScanEmitter scan-emitter, ^BufferAllocator allocator,
                                             ^RefCounter ref-ctr ^IWatermarkSource wm-src] :as deps}
                              {:keys [param-types default-tz table-info]}]

   (let [conformed-query (s/conform ::lp/logical-plan query)]
     (when (s/invalid? conformed-query)
       (throw (err/illegal-arg :malformed-query
                               {:plan query
                                :explain (s/explain-data ::lp/logical-plan query)})))

     (let [param-count (or (:param-count (meta query)) 0)
           param-types-with-defaults (->> (concat
                                           (mapv #(if (= :default %) :utf8 %) param-types)
                                           (repeat :utf8))
                                          (take param-count))
           tables (filter (comp #{:scan} :op) (lp/child-exprs conformed-query))
           scan-cols (->> tables
                          (into #{} (mapcat scan/->scan-cols)))

           _ (assert (or scan-emitter (empty? scan-cols)))

           relevant-schema-at-prepare-time
           (when (and table-info scan-emitter)
             (with-open [wm (.openWatermark wm-src)]
               (->> tables
                    (map #(str (get-in % [:scan-opts :table])))
                    (mapcat #(map (partial vector %) (get table-info %)))
                    (.scanFields scan-emitter wm))))

           cache (ConcurrentHashMap.)
           ordered-outer-projection (:named-projection (meta query))
           param-fields (mapify-params (mapv (comp types/col-type->field types/col-type->nullable-col-type) param-types-with-defaults))
           default-tz (or default-tz (.getZone expr/*clock*))]

       (reify PreparedQuery
         (paramFields [_] param-fields)
         (columnFields [_]
           (let [{:keys [fields]} (emit-expr cache deps conformed-query scan-cols default-tz (into {} param-fields))]
             ;; could store column-fields in the cache/map too
             (->column-fields ordered-outer-projection fields)))
         (bind [_ {:keys [args params basis default-tz]
                   :or {default-tz default-tz}}]

           ;; TODO throw if basis is in the future?
           (util/with-close-on-catch [args (open-args allocator args)]
             ;;TODO consider making the either/or relationship between params/args explicit, e.g throw error if both are provided
             (let [params (or params args)
                   {:keys [fields ->cursor]} (emit-expr cache deps conformed-query scan-cols default-tz (expr/->param-fields params))
                   {:keys [current-time]} basis
                   current-time (or current-time (.instant expr/*clock*))
                   clock (Clock/fixed current-time default-tz)]

               (reify
                 BoundQuery
                 (columnFields [_]
                   (->column-fields ordered-outer-projection fields))
                 (openCursor [_]
                   (when relevant-schema-at-prepare-time
                     (let [table-info-at-execution-time (with-open [wm (.openWatermark wm-src)]
                                                          (.scanFields scan-emitter wm
                                                                       (mapcat #(map (partial vector (key %)) (val %))
                                                                               (scan/tables-with-cols wm-src scan-emitter))))]

                       ;;TODO nullability of col is considered a schema change, not relevant for pgwire, maybe worth ignoring
                       ;;especially given our "per path schema" principal.
                       (when-not (= relevant-schema-at-prepare-time
                                    (select-keys table-info-at-execution-time (keys relevant-schema-at-prepare-time)))
                         (throw (err/runtime-err :prepared-query-out-of-date
                                                 ;;TODO consider adding the schema diff to the error, potentially quite large.
                                                 {::err/message "Relevant table schema has changed since preparing query, please prepare again"})))))
                   (.acquire ref-ctr)
                   (let [^BufferAllocator allocator
                         (if allocator
                           (util/->child-allocator allocator "BoundQuery/openCursor")
                           (RootAllocator.))
                         wm (some-> wm-src (.openWatermark))
]
                     (try
                       (binding [expr/*clock* clock]
                         (-> (->cursor {:allocator allocator, :watermark wm
                                        :clock clock,
                                        :basis (-> basis
                                                   (update :at-tx (fnil identity (some-> wm .txBasis)))
                                                   (assoc :current-time current-time))
                                        :params params})
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
         {:prepare-cache-size 1000 
          :plan-cache-size 1000
          :allocator (ig/ref :xtdb/allocator)
          :scan-emitter (ig/ref ::scan/scan-emitter)
          :metadata-mgr (ig/ref ::meta/metadata-manager)}))

(defn ->caffeine-cache ^com.github.benmanes.caffeine.cache.Cache [size]
  (-> (Caffeine/newBuilder) (.maximumSize size) (.build)))

(defmethod ig/init-key ::query-source [_ {:keys [plan-cache-size] :as deps}]
  (let [plan-cache (->caffeine-cache plan-cache-size)
        ref-ctr (RefCounter.)
        deps (-> deps (assoc :ref-ctr ref-ctr))]
    (reify
      IQuerySource
      (prepareRaQuery [_ query wm-src query-opts]
        (prepare-ra query (assoc deps :wm-src wm-src) (assoc query-opts :table-info (scan/tables-with-cols wm-src (:scan-emitter deps)))))
      (planQuery [_ query wm-src query-opts]
        (let [table-info (scan/tables-with-cols wm-src (:scan-emitter deps))
              plan-query-opts
              (-> query-opts
                  (select-keys
                   [:decorrelate? :explain? :instrument-rules? :project-anonymous-columns? :validate-plan?])
                  (update :decorrelate? #(if (nil? %) true false))
                  (assoc :table-info table-info))
              ;;TODO defaults to true in rewrite plan so needs defaulting pre-cache,
              ;;Move all defaulting to this level if/when everyone goes via planQuery
              cache-key (assoc plan-query-opts :query query)]
          (.get ^Cache plan-cache cache-key
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
