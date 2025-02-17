(ns xtdb.query
  (:require [clojure.pprint :as pp]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            xtdb.expression.pg
            xtdb.expression.temporal
            [xtdb.logical-plan :as lp]
            [xtdb.metadata :as meta]
            [xtdb.metrics :as metrics]
            xtdb.operator.apply
            xtdb.operator.arrow
            xtdb.operator.csv
            xtdb.operator.group-by
            xtdb.operator.join
            xtdb.operator.order-by
            xtdb.operator.patch
            xtdb.operator.project
            xtdb.operator.rename
            [xtdb.operator.scan :as scan]
            xtdb.operator.select
            xtdb.operator.set
            xtdb.operator.table
            xtdb.operator.top
            xtdb.operator.unnest
            xtdb.operator.window
            [xtdb.sql :as sql]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]
            [xtdb.xtql :as xtql]
            [xtdb.xtql.plan :as xtql.plan])
  (:import clojure.lang.MapEntry
           (com.github.benmanes.caffeine.cache Cache Caffeine)
           io.micrometer.core.instrument.Counter
           java.lang.AutoCloseable
           (java.time Duration InstantSource)
           (java.util HashMap)
           (java.util.concurrent ConcurrentHashMap)
           (java.util.function Function)
           [java.util.stream Stream StreamSupport]
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           org.apache.arrow.vector.types.pojo.Field
           (xtdb ICursor IResultCursor)
           (xtdb.antlr Sql$DirectlyExecutableStatementContext)
           (xtdb.api.query IKeyFn Query)
           (xtdb.indexer Watermark$Source)
           xtdb.metadata.IMetadataManager
           xtdb.operator.scan.IScanEmitter
           xtdb.util.RefCounter
           xtdb.vector.IVectorReader))

(definterface BoundQuery
  (^java.util.List columnFields [])
  (^xtdb.ICursor openCursor [])
  (^void close []
   "optional: if you close this BoundQuery it'll close any closed-over args relation"))

(definterface PreparedQuery
  (^java.util.List paramFields [])
  (^java.util.List columnFields [])
  (^java.util.List warnings [])
  (^xtdb.query.BoundQuery bind [queryOpts]
   "queryOpts :: {:args, :close-args?, :snapshot-time, :current-time, :default-tz}

    close-args? :: boolean, default true

    args :: RelationReader
      N.B. `args` will be closed when this BoundQuery closes"))

(definterface IQuerySource
  (^xtdb.query.PreparedQuery prepareRaQuery [ra-query query-opts])
  (^xtdb.query.PreparedQuery prepareRaQuery [ra-query wm-src query-opts])

  (^clojure.lang.PersistentVector planQuery [query query-opts])
  (^clojure.lang.PersistentVector planQuery [query wm-src query-opts]))

(defn- wrap-cursor ^xtdb.IResultCursor [^ICursor cursor, ^AutoCloseable wm, ^BufferAllocator al,
                                        current-time, snapshot-time, default-tz, ^RefCounter ref-ctr, fields]
  (reify IResultCursor
    (tryAdvance [_ c]
      (when (.isClosing ref-ctr)
        (throw (InterruptedException.)))

      (binding [expr/*clock* (InstantSource/fixed current-time)
                expr/*snapshot-time* snapshot-time
                expr/*default-tz* default-tz]
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

(defn emit-expr [^ConcurrentHashMap cache {:keys [^IScanEmitter scan-emitter, ^IMetadataManager metadata-mgr, ^Watermark$Source wm-src]}
                 conformed-query scan-cols default-tz param-fields]
  (.computeIfAbsent cache
                    {:scan-fields (when (and (seq scan-cols) scan-emitter)
                                    (with-open [wm (.openWatermark wm-src)]
                                      (.scanFields scan-emitter wm scan-cols)))
                     :default-tz default-tz
                     :last-known-block (when metadata-mgr
                                         (.latestBlockMetadata metadata-mgr))
                     :param-fields param-fields}
                    (reify Function
                      (apply [_ emit-opts]
                        (binding [expr/*default-tz* default-tz]
                          (lp/emit-expr conformed-query (assoc emit-opts :scan-emitter scan-emitter)))))))

(defn ->column-fields [ordered-outer-projection fields]
  (if ordered-outer-projection
    (->> ordered-outer-projection
         (mapv (fn [field-name]
                 (-> (get fields field-name)
                     (types/field-with-name (str field-name))))))
    (->> fields
         (mapv (fn [[field-name field]]
                 (types/field-with-name field (str field-name)))))))

(defn ->arg-fields [args]
  (->> args
       (into {} (map (fn [^IVectorReader col]
                       (MapEntry/create (symbol (.getName col)) (.getField col)))))))

(defn prepare-ra
  (^xtdb.query.PreparedQuery [query deps] (prepare-ra query deps {}))

  (^xtdb.query.PreparedQuery [query
                              {:keys [^IScanEmitter scan-emitter, ^BufferAllocator allocator,
                                      ^RefCounter ref-ctr ^Watermark$Source wm-src] :as deps}
                              {:keys [param-types default-tz table-info]}]

   (let [conformed-query (s/conform ::lp/logical-plan query)]
     (when (s/invalid? conformed-query)
       (log/debug "error conforming LP"
                  (pr-str {:plan query
                           :explain (s/explain-data ::lp/logical-plan query)}))
       (throw (err/illegal-arg :malformed-query {::err/message "internal error conforming query plan", :plan query})))

     (let [{:keys [ordered-outer-projection param-count warnings], :or {param-count 0}} (meta query)
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
           param-fields (->> param-types-with-defaults
                             (into [] (comp (map (comp types/col-type->field types/col-type->nullable-col-type))
                                            (map-indexed (fn [idx field]
                                                           (types/field-with-name field (str "?_" idx)))))))
           param-fields-by-name (into {} (map (juxt (comp symbol #(.getName ^Field %)) identity)) param-fields)
           default-tz (or default-tz expr/*default-tz*)]

       (reify PreparedQuery
         (paramFields [_] param-fields)
         (columnFields [_]
           (let [{:keys [fields]} (emit-expr cache deps conformed-query scan-cols default-tz param-fields-by-name)]
             ;; could store column-fields in the cache/map too
             (->column-fields ordered-outer-projection fields)))
         (warnings [_] warnings)

         (bind [_ {:keys [args current-time snapshot-time default-tz close-args?]
                   :or {default-tz default-tz
                        close-args? true}}]

           ;; TODO throw if basis is in the future?
           (let [{:keys [fields ->cursor]} (emit-expr cache deps conformed-query scan-cols default-tz (->arg-fields args))
                 current-time (or (some-> current-time time/->instant)
                                  (expr/current-time))]

             (reify
               BoundQuery
               (columnFields [_]
                 (->column-fields ordered-outer-projection fields))

               (openCursor [_]
                 (when relevant-schema-at-prepare-time
                   (let [table-info-at-execution-time (with-open [wm (.openWatermark wm-src)]
                                                        (.scanFields scan-emitter wm
                                                                     (mapcat #(map (partial vector (key %)) (val %))
                                                                             (scan/tables-with-cols wm-src))))]

                     ;;TODO nullability of col is considered a schema change, not relevant for pgwire, maybe worth ignoring
                     ;;especially given our "per path schema" principal.
                     (when-not (= relevant-schema-at-prepare-time
                                  (select-keys table-info-at-execution-time (keys relevant-schema-at-prepare-time)))
                       (throw (err/runtime-err :prepared-query-out-of-date
                                               ;;TODO consider adding the schema diff to the error, potentially quite large.
                                               {::err/message "Relevant table schema has changed since preparing query, please prepare again"})))))
                 (.acquire ref-ctr)
                 (util/with-close-on-catch [^BufferAllocator allocator
                                            (if allocator
                                              (util/->child-allocator allocator "BoundQuery/openCursor")
                                              (RootAllocator.))
                                            wm (.openWatermark wm-src)]
                   (try
                     (binding [expr/*clock* (InstantSource/fixed current-time)
                               expr/*default-tz* default-tz
                               expr/*snapshot-time* (or (some-> snapshot-time time/->instant)
                                                        (some-> wm .getTxBasis .getSystemTime))]
                       (-> (->cursor {:allocator allocator, :watermark wm
                                      :default-tz default-tz,
                                      :snapshot-time expr/*snapshot-time*
                                      :current-time current-time
                                      :args (or args vw/empty-args)
                                      :schema (scan/tables-with-cols wm-src)})
                           (wrap-cursor wm allocator current-time expr/*snapshot-time* default-tz ref-ctr fields)))

                     (catch Throwable t
                       (.release ref-ctr)
                       (throw t)))))

               AutoCloseable
               (close [_]
                 (when close-args?
                   (util/close args)))))))))))

(defmethod ig/prep-key ::query-source [_ opts]
  (merge opts
         {:plan-cache-size 1000
          :allocator (ig/ref :xtdb/allocator)
          :scan-emitter (ig/ref ::scan/scan-emitter)
          :metadata-mgr (ig/ref ::meta/metadata-manager)
          :live-idx (ig/ref :xtdb.indexer/live-index)
          :metrics-registry (ig/ref :xtdb.metrics/registry)}))

(defn ->caffeine-cache ^com.github.benmanes.caffeine.cache.Cache [size]
  (-> (Caffeine/newBuilder) (.maximumSize size) (.build)))

(defmethod ig/init-key ::query-source [_ {:keys [plan-cache-size live-idx metrics-registry] :as deps}]
  (let [plan-cache (->caffeine-cache plan-cache-size)
        ref-ctr (RefCounter.)
        deps (-> deps (assoc :ref-ctr ref-ctr))
        query-warning-counter ^Counter (metrics/add-counter metrics-registry "query.warning")]
    (reify
      IQuerySource
      (prepareRaQuery [this query query-opts]
        (.prepareRaQuery this query live-idx query-opts))
      (prepareRaQuery [_ query wm-src query-opts]
        (let [prepared-query (prepare-ra query (assoc deps :wm-src wm-src) (assoc query-opts :table-info (scan/tables-with-cols wm-src)))]
          (when (seq (.warnings prepared-query))
            (.increment query-warning-counter))
          prepared-query))
      (planQuery [this query query-opts]
        (.planQuery this query live-idx query-opts))
      (planQuery [_ query wm-src query-opts]
        (let [table-info (scan/tables-with-cols wm-src)
              plan-query-opts
              (-> query-opts
                  (select-keys
                   [:decorrelate? :explain? :instrument-rules? :project-anonymous-columns? :validate-plan?])
                  (update :decorrelate? #(if (nil? %) true false))
                  (assoc :table-info table-info))]

          ;;TODO defaults to true in rewrite plan so needs defaulting pre-cache,
          ;;Move all defaulting to this level if/when everyone goes via planQuery
          (.get ^Cache plan-cache (assoc plan-query-opts :query query)
                (fn [_cache-key]
                  (let [plan (cond
                               (or (string? query) (instance? Sql$DirectlyExecutableStatementContext query))
                               (sql/compile-query query plan-query-opts)

                               (seq? query) (-> (xtql/parse-query query)
                                                (xtql.plan/compile-query plan-query-opts))

                               (instance? Query query) (xtql.plan/compile-query query plan-query-opts)

                               :else (throw (err/illegal-arg :unknown-query-type {:query query, :type (type query)})))]
                    (if (:explain? query-opts)
                      [:table [{:plan (with-out-str (pp/pprint plan))}]]
                      plan))))))

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

(defn wrapping-error-count-cursor [^IResultCursor source ^Counter query-error-counter]
  (reify IResultCursor
    (tryAdvance [_ c]
      (try
        (.tryAdvance source c)
        (catch Exception e
          (when query-error-counter
            (.increment query-error-counter))
          (throw e))))

    (characteristics [_] (.characteristics source))
    (estimateSize [_] (.estimateSize source))
    (getComparator [_] (.getComparator source))
    (getExactSizeIfKnown [_] (.getExactSizeIfKnown source))
    (hasCharacteristics [_ c] (.hasCharacteristics source c))
    (trySplit [_] (.trySplit source))
    (close [_] (.close source))
    (columnFields [_] (.columnFields source))))

(defn open-cursor-as-stream
  (^java.util.stream.Stream [^BoundQuery bound-query query-opts]
   (open-cursor-as-stream bound-query query-opts {}))
  (^java.util.stream.Stream [^BoundQuery bound-query {:keys [key-fn]} {:keys [^Counter query-error-counter]}]
   (let [key-fn (cache-key-fn key-fn)]
     (util/with-close-on-catch [cursor (.openCursor bound-query)]
       (-> (StreamSupport/stream (cond-> cursor
                                   query-error-counter (wrapping-error-count-cursor query-error-counter)) false)
           ^Stream (.onClose (fn []
                               (util/close cursor)
                               (util/close bound-query)))
           (.flatMap (reify Function
                       (apply [_ rel]
                         (.stream (vr/rel->rows rel key-fn))))))))))
