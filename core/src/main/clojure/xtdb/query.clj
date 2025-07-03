(ns xtdb.query
  (:require [clojure.pprint :as pp]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            xtdb.expression.pg
            xtdb.expression.temporal
            xtdb.expression.uri
            [xtdb.logical-plan :as lp]
            [xtdb.metrics :as metrics]
            xtdb.operator.apply
            xtdb.operator.arrow
            xtdb.operator.csv
            xtdb.operator.group-by
            xtdb.operator.join
            xtdb.operator.list
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
           (xtdb ICursor IResultCursor IResultCursor$ErrorTrackingCursor)
           (xtdb.antlr Sql$DirectlyExecutableStatementContext)
           (xtdb.api.query IKeyFn Query)
           (xtdb.arrow RelationReader VectorReader)
           xtdb.catalog.BlockCatalog
           (xtdb.indexer Watermark Watermark$Source)
           xtdb.operator.scan.IScanEmitter
           (xtdb.query IQuerySource PreparedQuery)
           xtdb.util.RefCounter))

(defn- wrap-cursor ^xtdb.IResultCursor [^ICursor cursor, result-fields
                                        current-time, snapshot-time, default-tz,
                                        ^AutoCloseable wm, ^BufferAllocator al, ^RefCounter ref-ctr, args-to-close]
  (reify IResultCursor
    (getResultFields [_] result-fields)
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
      (util/close args-to-close)
      (util/close wm)
      (util/close al))))

(defn emit-expr [^ConcurrentHashMap cache {:keys [^IScanEmitter scan-emitter, ^BlockCatalog block-cat, table-cat]} wm
                 conformed-query scan-cols
                 default-tz param-fields]
  (.computeIfAbsent cache
                    {:scan-fields (when (seq scan-cols)
                                    (scan/scan-fields table-cat wm scan-cols))
                     :default-tz default-tz
                     :last-known-block (some-> block-cat .getCurrentBlockIndex)
                     :param-fields param-fields}
                    (fn [emit-opts]
                      (binding [expr/*default-tz* default-tz]
                        (lp/emit-expr conformed-query (assoc emit-opts :scan-emitter scan-emitter))))))

(defn- ->result-fields [ordered-outer-projection fields]
  (if ordered-outer-projection
    (->> ordered-outer-projection
         (mapv (fn [field-name]
                 (-> (get fields field-name)
                     (types/field-with-name (str field-name))))))
    (->> fields
         (mapv (fn [[field-name field]]
                 (types/field-with-name field (str field-name)))))))

(defn ->arg-fields [^RelationReader args]
  (->> args
       (into {} (map (fn [^VectorReader col]
                       (MapEntry/create (symbol (.getName col)) (.getField col)))))))

(defn expr->instant [expr {:keys [^RelationReader args default-tz] :as opts}]
  (if (symbol? expr)
    (recur (-> args (.vectorFor (str expr)) (.getObject 0)) opts)
    (time/->instant expr {:default-tz default-tz})))

(defn- validate-snapshot-not-before [snapshot-time ^Watermark wm]
  (let [wm-tx (.getTxBasis wm)]
    (when (and snapshot-time (or (nil? wm-tx) (neg? (compare (.getSystemTime wm-tx) snapshot-time))))
      (throw (err/illegal-arg :xtdb/unindexed-tx
                              {::err/message (format "snapshot-time (%s) is after the latest completed tx (%s)"
                                                     (str snapshot-time) (pr-str wm-tx))
                               :latest-completed-tx wm-tx
                               :snapshot-time snapshot-time})))))

(defn prepare-ra
  (^xtdb.query.PreparedQuery [query deps] (prepare-ra query deps {}))

  (^xtdb.query.PreparedQuery [query
                              {:keys [^BufferAllocator allocator, ^RefCounter ref-ctr, ^Watermark$Source wm-src] :as deps}
                              {:keys [default-tz]}]

   (let [conformed-query (s/conform ::lp/logical-plan query)]
     (when (s/invalid? conformed-query)
       (log/warn "error conforming LP"
                 (pr-str {:plan query
                          :explain (s/explain-data ::lp/logical-plan query)}))
       (throw (err/incorrect ::invalid-query-plan "internal error conforming query plan"
                             {:plan query})))

     (let [tables (filter (comp #{:scan} :op) (lp/child-exprs conformed-query))
           scan-cols (->> tables
                          (into #{} (mapcat scan/->scan-cols)))

           cache (ConcurrentHashMap.)

           default-tz (or default-tz expr/*default-tz*)

           {:keys [ordered-outer-projection warnings param-count], :or {param-count 0} :as plan-meta} (meta query)]

       (reify PreparedQuery
         (getParamCount [_] param-count)
         (getColumnNames [_] ordered-outer-projection)

         (getColumnFields [_ param-fields]
           (let [param-fields-by-name (->> param-fields
                                           (into {} (map (juxt (comp symbol #(.getName ^Field %)) identity))))
                 {:keys [fields]} (with-open [wm (.openWatermark wm-src)]
                                    (emit-expr cache deps wm conformed-query scan-cols default-tz param-fields-by-name))]
             ;; could store column-fields in the cache/map too
             (->result-fields ordered-outer-projection fields)))

         (getWarnings [_] warnings)

         (openQuery [_ {:keys [args current-time snapshot-time default-tz close-args? after-tx-id]
                        :or {default-tz default-tz
                             close-args? true}}]
           (util/with-close-on-catch [^BufferAllocator allocator (if allocator
                                                                   (util/->child-allocator allocator "BoundQuery/openCursor")
                                                                   (RootAllocator.))
                                      wm (.openWatermark wm-src)]
             (let [args (cond
                          (instance? RelationReader args) args
                          (vector? args) (vw/open-args allocator args)
                          (nil? args) vw/empty-args
                          :else (throw (ex-info "invalid args"
                                                {:type (class args)})))
                   {:keys [fields ->cursor]} (emit-expr cache deps wm conformed-query scan-cols default-tz (->arg-fields args))
                   current-time (or (some-> (:current-time plan-meta) (expr->instant {:args args, :default-tz default-tz}))
                                    (some-> current-time (expr->instant {:args args, :default-tz default-tz}))
                                    (expr/current-time))]

               (.acquire ref-ctr)
               (try
                 (binding [expr/*clock* (InstantSource/fixed current-time)
                           expr/*default-tz* default-tz
                           expr/*snapshot-time* (or (some-> (:snapshot-time plan-meta) (expr->instant {:args args, :default-tz default-tz}))
                                                    (some-> snapshot-time (expr->instant {:args args, :default-tz default-tz}))
                                                    (some-> wm .getTxBasis .getSystemTime))
                           expr/*after-tx-id* (or after-tx-id -1)]

                   (validate-snapshot-not-before expr/*snapshot-time* wm)

                   (-> (->cursor {:allocator allocator, :watermark wm
                                  :default-tz default-tz,
                                  :snapshot-time expr/*snapshot-time*
                                  :current-time current-time
                                  :args (or args vw/empty-args)
                                  :schema (some-> wm .getSchema)})
                       (wrap-cursor (->result-fields ordered-outer-projection fields)
                                    current-time expr/*snapshot-time* default-tz
                                    allocator wm ref-ctr (when close-args? args))))

                 (catch Throwable t
                   (.release ref-ctr)
                   (when close-args?
                     (util/close args))
                   (throw t)))))))))))

(defmethod ig/prep-key ::query-source [_ opts]
  (merge opts
         {:plan-cache-size 1000
          :allocator (ig/ref :xtdb/allocator)
          :scan-emitter (ig/ref ::scan/scan-emitter)
          :live-idx (ig/ref :xtdb.indexer/live-index)
          :table-cat (ig/ref :xtdb/table-catalog)
          :metrics-registry (ig/ref :xtdb.metrics/registry)}))

(defn ->caffeine-cache ^com.github.benmanes.caffeine.cache.Cache [size]
  (-> (Caffeine/newBuilder) (.maximumSize size) (.build)))

(defmethod ig/init-key ::query-source [_ {:keys [allocator plan-cache-size live-idx metrics-registry] :as deps}]
  (let [plan-cache (->caffeine-cache plan-cache-size)
        ref-ctr (RefCounter.)
        deps (-> deps (assoc :ref-ctr ref-ctr))
        ^Counter query-warning-counter (metrics/add-counter metrics-registry "query.warning")
        allocator (util/->child-allocator allocator "query-source")]

    (metrics/add-allocator-gauge metrics-registry "query_source.allocator.allocated_memory" allocator)

    (reify
      IQuerySource
      (planQuery [this query query-opts]
        (.planQuery this query live-idx query-opts))

      (planQuery [_ query wm-src query-opts]
        (let [table-info (scan/tables-with-cols wm-src)
              plan-query-opts
              (-> query-opts
                  (select-keys [:decorrelate? :explain? :instrument-rules? :project-anonymous-columns? :validate-plan? :arg-fields])
                  (update :decorrelate? #(if (nil? %) true false))
                  (assoc :table-info table-info))]

          ;;TODO defaults to true in rewrite plan so needs defaulting pre-cache,
          ;;Move all defaulting to this level if/when everyone goes via planQuery
          (.get ^Cache plan-cache (assoc plan-query-opts :query query)
                (fn [_cache-key]
                  (let [plan (cond
                               (or (string? query) (instance? Sql$DirectlyExecutableStatementContext query))
                               (sql/plan query plan-query-opts)

                               (seq? query) (-> (xtql/parse-query query)
                                                (xtql.plan/compile-query plan-query-opts))

                               (instance? Query query) (xtql.plan/compile-query query plan-query-opts)

                               :else (throw (err/illegal-arg :unknown-query-type {:query query, :type (type query)})))]
                    (if (or (:explain? query-opts) (:explain? (meta plan)))
                      (with-meta [:table [{:plan (with-out-str (pp/pprint plan))}]]
                        (-> (meta plan) (select-keys [:param-count :warnings])))
                      plan))))))

      (prepareRaQuery [this query query-opts]
        (.prepareRaQuery this query live-idx query-opts))

      (prepareRaQuery [_ query wm-src query-opts]
        (let [prepared-query (prepare-ra query (assoc deps :allocator allocator, :wm-src wm-src) (assoc query-opts :table-info (scan/tables-with-cols wm-src)))]
          (when (seq (.getWarnings prepared-query))
            (.increment query-warning-counter))
          prepared-query))

      AutoCloseable
      (close [_]
        (when-not (.tryClose ref-ctr (Duration/ofMinutes 1))
          (log/warn "Failed to shut down after 60s due to outstanding queries"))

        (util/close allocator)))))

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

(defn cursor->stream
  (^java.util.stream.Stream [^IResultCursor cursor query-opts]
   (cursor->stream cursor query-opts {}))
  (^java.util.stream.Stream [^IResultCursor cursor {:keys [key-fn]} {:keys [query-error-counter]}]
   (let [key-fn (cache-key-fn key-fn)]
     (-> (StreamSupport/stream (cond-> cursor
                                 query-error-counter (IResultCursor$ErrorTrackingCursor. query-error-counter))
                               false)
         ^Stream (.onClose (fn []
                             (util/close cursor)))
         (.flatMap (fn [^RelationReader rel]
                     (.stream (.toMaps rel key-fn))))))))
