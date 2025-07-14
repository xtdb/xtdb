(ns xtdb.query
  (:require [clojure.pprint :as pp]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.antlr :as antlr]
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
           (java.util.function Function)
           [java.util.stream Stream StreamSupport]
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           org.apache.arrow.vector.types.pojo.Field
           (xtdb ICursor IResultCursor IResultCursor$ErrorTrackingCursor)
           (xtdb.antlr Sql$DirectlyExecutableStatementContext)
           (xtdb.api.query IKeyFn Query)
           (xtdb.arrow RelationReader VectorReader)
           xtdb.catalog.BlockCatalog
           (xtdb.indexer Watermark)
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

(defn- parse-query [query]
  (cond
    (vector? query) query
    (string? query) (antlr/parse-statement query)
    (instance? Sql$DirectlyExecutableStatementContext query) query
    (seq? query) (xtql/parse-query query)
    (instance? Query query) query

    :else (throw (err/incorrect :unknown-query-type "Unknown query type"
                                {:query query, :type (type query)}))))

(defn- plan-query [parsed-query query-opts]
  (let [plan (cond
               (vector? parsed-query) parsed-query
               (instance? Sql$DirectlyExecutableStatementContext parsed-query) (sql/plan parsed-query query-opts)
               (instance? Query parsed-query) (xtql.plan/compile-query parsed-query query-opts)

               :else (throw (err/fault :unknown-query-type "Unknown query type"
                                       {:query parsed-query, :type (class parsed-query)})))]

    (if (or (:explain? query-opts) (:explain? (meta plan)))
      (with-meta [:table [{:plan (with-out-str (pp/pprint plan))}]]
        (-> (meta plan) (select-keys [:param-count :warnings])))
      plan)))

(defn- conform-plan [plan]
  (let [conformed-plan (s/conform ::lp/logical-plan plan)]
    (when (s/invalid? conformed-plan)
      (log/warn "error conforming LP"
                (pr-str {:plan plan, :explain (s/explain-data ::lp/logical-plan plan)}))

      (throw (err/incorrect ::invalid-query-plan "internal error conforming query plan"
                            {:plan plan})))

    conformed-plan))

(defprotocol PQuerySource
  (-plan-query [q-src parsed-query query-opts table-info])
  (-emit-query [q-src planned-query wm param-fields default-tz]))

(defrecord QuerySource [^BufferAllocator allocator, wm-src, scan-emitter
                        ^BlockCatalog block-cat, table-cat
                        ^Counter query-warning-counter
                        ^RefCounter ref-ctr
                        ^Cache plan-cache]
  IQuerySource
  PQuerySource
  (-plan-query [_ parsed-query query-opts table-info]
    (.get plan-cache [parsed-query (-> query-opts
                                       (select-keys [:decorrelate? :explain? :arg-fields])
                                       (update :decorrelate? (fnil boolean true))
                                       (assoc :table-info table-info))]
          (fn [[parsed-query query-opts]]
            (let [plan (plan-query parsed-query query-opts)
                  conformed-plan (conform-plan plan)

                  {:keys [ordered-outer-projection warnings param-count], :or {param-count 0}, :as plan-meta} (meta plan)]

              (into (select-keys plan-meta [:current-time :snapshot-time])
                    {:plan plan,
                     :conformed-plan conformed-plan
                     :scan-cols (->> (lp/child-exprs conformed-plan)
                                     (filter (comp #{:scan} :op))
                                     (into #{} (mapcat scan/->scan-cols)))
                     :col-names ordered-outer-projection
                     :warnings warnings
                     :param-count param-count
                     :emit-cache (-> (Caffeine/newBuilder)
                                     (.maximumSize 16)
                                     (.build))})))))

  (-emit-query [_ {:keys [conformed-plan scan-cols col-names ^Cache emit-cache]} wm param-fields default-tz]
    (.get emit-cache {:scan-fields (when (seq scan-cols)
                                     (scan/scan-fields table-cat wm scan-cols))
                      :last-known-block (some-> block-cat .getCurrentBlockIndex)
                      :default-tz default-tz
                      :param-fields param-fields}

          (fn [{:keys [scan-fields last-known-block param-fields default-tz]}]
            (let [{:keys [fields ->cursor]} (binding [expr/*default-tz* default-tz]
                                              (lp/emit-expr conformed-plan
                                                            {:scan-fields scan-fields
                                                             :default-tz default-tz
                                                             :last-known-block last-known-block
                                                             :param-fields param-fields
                                                             :scan-emitter scan-emitter}))]

              {:fields (->result-fields col-names fields)
               :->cursor ->cursor}))))

  (prepareQuery [this query query-opts]
    (.prepareQuery this query wm-src query-opts))

  (prepareQuery [this query wm-src {:keys [default-tz] :as query-opts}]
    (let [parsed-query (parse-query query)
          !table-info (atom (scan/tables-with-cols wm-src))
          default-tz (or default-tz expr/*default-tz*)]
      (letfn [(plan-query* [table-info]
                (-plan-query this parsed-query query-opts table-info))]

        (reify PreparedQuery
          (getParamCount [_] (:param-count (plan-query* @!table-info)))
          (getColumnNames [_] (:col-names (plan-query* @!table-info)))

          (getColumnFields [_ param-fields]
            (let [planned-query (plan-query* @!table-info)]
              (with-open [wm (.openWatermark wm-src)]
                (:fields (-emit-query this planned-query wm
                                      (->> param-fields
                                           (into {} (map (juxt (comp symbol #(.getName ^Field %)) identity))))
                                      default-tz)))))

          (getWarnings [_] (:warnings (plan-query* @!table-info)))

          (openQuery [_ {:keys [args current-time snapshot-time default-tz close-args? after-tx-id]
                         :or {default-tz default-tz
                              close-args? true}}]
            (util/with-close-on-catch [^BufferAllocator allocator (if allocator
                                                                    (util/->child-allocator allocator "BoundQuery/openCursor")
                                                                    (RootAllocator.))
                                       wm (.openWatermark wm-src)]
              (let [table-info (reset! !table-info (.getSchema wm))
                    planned-query (plan-query* table-info)
                    args (cond
                           (instance? RelationReader args) args
                           (vector? args) (vw/open-args allocator args)
                           (nil? args) vw/empty-args
                           :else (throw (ex-info "invalid args"
                                                 {:type (class args)})))

                    {:keys [fields ->cursor]} (-emit-query this planned-query wm (->arg-fields args) default-tz)
                    current-time (or (some-> (:current-time planned-query) (expr->instant {:args args, :default-tz default-tz}))
                                     (some-> current-time (expr->instant {:args args, :default-tz default-tz}))
                                     (expr/current-time))]

                (when (seq (:warnings planned-query))
                  (.increment query-warning-counter))

                (.acquire ref-ctr)
                (try
                  (binding [expr/*clock* (InstantSource/fixed current-time)
                            expr/*default-tz* default-tz
                            expr/*snapshot-time* (or (some-> (:snapshot-time planned-query) (expr->instant {:args args, :default-tz default-tz}))
                                                     (some-> snapshot-time (expr->instant {:args args, :default-tz default-tz}))
                                                     (some-> wm .getTxBasis .getSystemTime))
                            expr/*after-tx-id* (or after-tx-id -1)]

                    (validate-snapshot-not-before expr/*snapshot-time* wm)

                    (-> (->cursor {:allocator allocator, :watermark wm
                                   :default-tz default-tz,
                                   :snapshot-time expr/*snapshot-time*
                                   :current-time current-time
                                   :args args
                                   :schema table-info})
                        (wrap-cursor fields
                                     current-time expr/*snapshot-time* default-tz
                                     allocator wm ref-ctr (when close-args? args))))

                  (catch Throwable t
                    (.release ref-ctr)
                    (when close-args?
                      (util/close args))
                    (throw t))))))))))

  AutoCloseable
  (close [_]
    (when-not (.tryClose ref-ctr (Duration/ofMinutes 1))
      (log/warn "Failed to shut down after 60s due to outstanding queries"))

    (util/close allocator)))

(defmethod ig/prep-key ::query-source [_ opts]
  (merge opts
         {:allocator (ig/ref :xtdb/allocator)
          :scan-emitter (ig/ref ::scan/scan-emitter)
          :wm-src (ig/ref :xtdb.indexer/live-index)
          :table-cat (ig/ref :xtdb/table-catalog)
          :metrics-registry (ig/ref :xtdb.metrics/registry)}))

(defn ->query-source [{:keys [allocator metrics-registry] :as deps}]
  (let [ref-ctr (RefCounter.)
        allocator (util/->child-allocator allocator "query-source")
        deps (-> deps
                 (assoc :ref-ctr ref-ctr
                        :query-warning-counter (some-> metrics-registry (metrics/add-counter "query.warning"))
                        :allocator allocator
                        :plan-cache (-> (Caffeine/newBuilder)
                                        (.maximumSize 4096)
                                        (.build))))]

    (some-> metrics-registry (metrics/add-allocator-gauge "query_source.allocator.allocated_memory" allocator))

    (map->QuerySource deps)))

(defmethod ig/init-key ::query-source [_ deps]
  (->query-source deps))

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
