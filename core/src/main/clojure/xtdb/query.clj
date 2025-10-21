(ns xtdb.query
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.antlr :as antlr]
            [xtdb.basis :as basis]
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
            xtdb.operator.distinct
            xtdb.operator.group-by
            xtdb.operator.join
            xtdb.operator.let
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
            [xtdb.serde.types :as serde-types]
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
           [java.util.concurrent.atomic AtomicBoolean]
           (java.util.function Function)
           [java.util.stream Stream StreamSupport]
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           org.apache.arrow.vector.types.pojo.Field
           (xtdb ICursor IResultCursor IResultCursor$ErrorTrackingCursor PagesCursor)
           (xtdb.antlr Sql$DirectlyExecutableStatementContext)
           (xtdb.api.query IKeyFn Query)
           (xtdb.arrow RelationReader VectorReader)
           (xtdb.database Database$Catalog)
           (xtdb.indexer Snapshot)
           xtdb.operator.scan.IScanEmitter
           (xtdb.query IQuerySource PreparedQuery)
           xtdb.util.RefCounter))

(defn- wrap-result-fields [^ICursor cursor, result-fields]
  (reify IResultCursor
    (getResultFields [_] result-fields)
    (tryAdvance [_ c] (.tryAdvance cursor c))

    (getCursorType [_] (.getCursorType cursor))
    (getChildCursors [_] (.getChildCursors cursor))
    (getExplainAnalyze [_] (.getExplainAnalyze cursor))

    (characteristics [_] (.characteristics cursor))
    (estimateSize [_] (.estimateSize cursor))
    (getComparator [_] (.getComparator cursor))
    (getExactSizeIfKnown [_] (.getExactSizeIfKnown cursor))
    (hasCharacteristics [_ c] (.hasCharacteristics cursor c))
    (trySplit [_] (.trySplit cursor))
    (close [_] (.close cursor))))

(defn- wrap-dynvars ^xtdb.IResultCursor [^IResultCursor cursor
                                         current-time, snapshot-token, default-tz
                                         ^RefCounter ref-ctr]
  (reify IResultCursor
    (getResultFields [_] (.getResultFields cursor))
    (tryAdvance [_ c]
      (when (.isClosing ref-ctr)
        (throw (InterruptedException.)))

      (binding [expr/*clock* (InstantSource/fixed current-time)
                expr/*snapshot-token* snapshot-token
                expr/*default-tz* default-tz]
        (.tryAdvance cursor c)))

    (getCursorType [_] (.getCursorType cursor))
    (getChildCursors [_] (.getChildCursors cursor))
    (getExplainAnalyze [_] (.getExplainAnalyze cursor))

    (characteristics [_] (.characteristics cursor))
    (estimateSize [_] (.estimateSize cursor))
    (getComparator [_] (.getComparator cursor))
    (getExactSizeIfKnown [_] (.getExactSizeIfKnown cursor))
    (hasCharacteristics [_ c] (.hasCharacteristics cursor c))
    (trySplit [_] (.trySplit cursor))
    (close [_] (.close cursor))))

(defn- wrap-closeables ^xtdb.IResultCursor [^IResultCursor cursor, ^RefCounter ref-ctr, closeables]
  (let [!closed? (AtomicBoolean. false)]
    (reify IResultCursor
      (getResultFields [_] (.getResultFields cursor))
      (tryAdvance [_ c] (.tryAdvance cursor c))

      (getCursorType [_] (.getCursorType cursor))
      (getChildCursors [_] (.getChildCursors cursor))
      (getExplainAnalyze [_] (.getExplainAnalyze cursor))

      (characteristics [_] (.characteristics cursor))
      (estimateSize [_] (.estimateSize cursor))
      (getComparator [_] (.getComparator cursor))
      (getExactSizeIfKnown [_] (.getExactSizeIfKnown cursor))
      (hasCharacteristics [_ c] (.hasCharacteristics cursor c))
      (trySplit [_] (.trySplit cursor))

      (close [_]
        (when (.compareAndSet !closed? false true)
          (.close cursor)
          (some-> ref-ctr .release)
          (util/close closeables))))))

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

(defn expr->value [expr {:keys [^RelationReader args]}]
  (if (symbol? expr)
    (-> args (.vectorFor (str expr)) (.getObject 0))
    expr))

(defn- validate-basis-not-before [basis snaps]
  (doseq [[db-name ^Snapshot snap] snaps]
    (let [snap-tx (.getTxBasis snap)
          system-time (get-in basis [db-name 0])]
      (when (and system-time
                 (or (nil? snap-tx)
                     (neg? (compare (.getSystemTime snap-tx) system-time))))
        (throw (err/incorrect :xtdb/unindexed-tx
                              (format "system-time (%s) is after the latest completed tx (%s)"
                                      (str system-time) (pr-str snap-tx))
                              {:latest-completed-tx snap-tx
                               :system-time system-time}))))))

(defn- default-basis [snaps]
  (->> (for [[db-name ^Snapshot snap] snaps
             :let [sys-time (some-> (.getTxBasis snap) (.getSystemTime))]
             :when sys-time]
         [db-name [sys-time]])
       (into {})
       (not-empty)))

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
  (cond
    (vector? parsed-query) parsed-query
    (instance? Sql$DirectlyExecutableStatementContext parsed-query) (sql/plan parsed-query query-opts)
    (instance? Query parsed-query) (xtql.plan/compile-query parsed-query query-opts)

    :else (throw (err/fault :unknown-query-type "Unknown query type"
                            {:query parsed-query, :type (class parsed-query)}))))

(defn- conform-plan [plan]
  (let [conformed-plan (s/conform ::lp/logical-plan plan)]
    (when (s/invalid? conformed-plan)
      (log/warn "error conforming LP"
                (pr-str {:plan plan, :explain (s/explain-data ::lp/logical-plan plan)}))

      (throw (err/incorrect ::invalid-query-plan "internal error conforming query plan"
                            {:plan plan})))

    conformed-plan))

(defn- emit-query [{:keys [conformed-plan scan-cols col-names ^Cache emit-cache, explain-analyze?]},
                   scan-emitter, ^Database$Catalog db-cat, snaps
                   param-fields, {:keys [default-tz]}]
  (.get emit-cache {:scan-fields (scan/scan-fields db-cat snaps scan-cols)

                    ;; this one is just to reset the cache for up-to-date stats
                    ;; probably over-zealous
                    :last-known-blocks (->> (for [db-name (.getDatabaseNames db-cat)]
                                              [db-name (some-> (.databaseOrNull db-cat db-name) .getBlockCatalog .getCurrentBlockIndex)]))

                    :default-tz default-tz
                    :param-fields param-fields
                    :explain-analyze? explain-analyze?}

        (fn [{:keys [scan-fields param-fields default-tz]}]
          (binding [expr/*default-tz* default-tz]
            (-> (lp/emit-expr conformed-plan
                              {:scan-fields scan-fields
                               :default-tz default-tz
                               :param-fields param-fields
                               :db-cat db-cat
                               :scan-emitter scan-emitter})
                (update :fields (fn [fs]
                                  (->result-fields col-names fs))))))))

(defn- ->explain-plan [emitted-expr]
  (letfn [(->explain-plan* [{:keys [op children explain]} depth]
            (lazy-seq
             (cons {:depth (str (str/join (repeat depth "  ")) "->")
                    :op op
                    :explain explain}
                   (->> (for [child children]
                          (->explain-plan* child (inc depth)))
                        (sequence cat)))))]

    (->explain-plan* emitted-expr 0)))

(defn- explain-plan-fields [explain-plan]
  (let [[_struct col-types] (->> (map vw/value->col-type explain-plan)
                                 distinct
                                 (apply types/merge-col-types))]
    (for [field-name '[depth op explain]]
      (types/col-type->field field-name (get col-types field-name)))))

(def ^:private explain-analyze-fields
  ;; for some reason we're not able to use the reader macro here.
  ;; it's a bug with either Clojure AOT or Gradle Clojurephant, not sure which.
  [(serde-types/->field ["depth" :utf8])
   (serde-types/->field ["op" :keyword])

   (serde-types/->field ["total_time" [:duration :micro]])
   (serde-types/->field ["time_to_first_block" [:duration :micro]])

   (serde-types/->field ["block_count" :i64])
   (serde-types/->field ["row_count" :i64])])

(defn- explain-analyze-results [^IResultCursor cursor]
  (letfn [(->results [^ICursor cursor, depth]
            (lazy-seq
             (if-let [ea (.getExplainAnalyze cursor)]
               (cons {:depth (str (str/join (repeat depth "  ")) "->")
                      :op (keyword (.getCursorType cursor))
                      :total-time (.getTotalTime ea)
                      :time-to-first-block (.getTimeToFirstBlock ea)
                      :block-count (.getBlockCount ea)
                      :row-count (.getRowCount ea)}
                     (->> (for [child (.getChildCursors cursor)]
                            (->results child (inc depth)))
                          (sequence cat)))
               (->results (first (.getChildCursors cursor)) depth))))]

    (vec (->results cursor 0))))

(defprotocol PQuerySource
  (-plan-query [q-src parsed-query query-opts table-info]))

(defrecord QuerySource [^BufferAllocator allocator
                        ^IScanEmitter scan-emitter
                        ^Counter query-warning-counter
                        ^RefCounter ref-ctr
                        ^Cache plan-cache]
  PQuerySource
  (-plan-query [_ parsed-query query-opts table-info]
    (.get plan-cache [parsed-query (-> query-opts
                                       (select-keys [:default-db :decorrelate? :explain? :explain-analyze? :arg-fields])
                                       (update :decorrelate? (fnil boolean true))
                                       (assoc :table-info table-info))]
          (fn [[parsed-query query-opts]]
            (let [plan (plan-query parsed-query query-opts)
                  conformed-plan (conform-plan plan)

                  {:keys [ordered-outer-projection warnings param-count], :or {param-count 0}, :as plan-meta} (meta plan)]

              (into (select-keys plan-meta [:current-time :snapshot-token :snapshot-time
                                            :explain? :explain-analyze?])
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

  IQuerySource
  (prepareQuery [this query db-cat query-opts]
    (let [parsed-query (parse-query query)

          {:keys [default-tz] :as query-opts} (-> query-opts
                                                  (update :default-tz (fnil identity expr/*default-tz*)))]
      (letfn [(open-snaps []
                ;; TODO this opens up a snapshot for *every* db in the catalog
                ;; when people have 'proper' multi-tenancy, this will be a problem
                ;; thankfully we can probably figure out what databases may be relevant to the query at parse-time
                (util/with-close-on-catch [!snaps (HashMap.)]
                  (doseq [db-name (.getDatabaseNames db-cat)]
                    (.put !snaps db-name (.openSnapshot (.getSnapSource (.databaseOrNull db-cat db-name)))))
                  (into {} !snaps)))

              (->table-info []
                ;; TODO this, too, gets the schema for *every* db in the catalog
                (->> (.getDatabaseNames db-cat)
                     (into {} (mapcat (fn [db-name]
                                        (util/with-open [snap (.openSnapshot (.getSnapSource (.databaseOrNull db-cat db-name)))]
                                          (.getSchema snap)))))))

              (plan-query* [table-info]
                (-plan-query this parsed-query query-opts table-info))]

        (let [!table-info (atom (->table-info))]

          (reify PreparedQuery
            (getParamCount [_] (:param-count (plan-query* @!table-info)))
            (getColumnNames [_] (:col-names (plan-query* @!table-info)))

            (getColumnFields [_ param-fields]
              (let [planned-query (plan-query* @!table-info)]
                (util/with-open [snaps (open-snaps)]
                  (let [emitted-query (emit-query planned-query scan-emitter db-cat snaps
                                                  (->> param-fields
                                                       (into {} (map (juxt (comp symbol #(.getName ^Field %)) identity))))
                                                  query-opts)]
                    (cond
                      (:explain? planned-query) (explain-plan-fields (->explain-plan emitted-query))
                      (:explain-analyze? planned-query) explain-analyze-fields
                      :else (:fields emitted-query))))))

            (getWarnings [_] (:warnings (plan-query* @!table-info)))

            (openQuery [_ {:keys [args current-time snapshot-token snapshot-time default-tz close-args? await-token]
                           :or {default-tz default-tz
                                close-args? true}}]
              (util/with-close-on-catch [^BufferAllocator allocator (if allocator
                                                                      (util/->child-allocator allocator "BoundQuery/openCursor")
                                                                      (RootAllocator.))
                                         snaps (open-snaps)]
                (let [query-opts (-> query-opts (assoc :default-tz default-tz))
                      table-info (reset! !table-info (->table-info))
                      planned-query (plan-query* table-info)
                      args (cond
                             (instance? RelationReader args) args
                             (vector? args) (vw/open-args allocator args)
                             (nil? args) vw/empty-args
                             :else (throw (ex-info "invalid args"
                                                   {:type (class args)})))

                      {:keys [fields ->cursor] :as emitted-query} (emit-query planned-query scan-emitter db-cat snaps (->arg-fields args) query-opts)
                      current-time (or (some-> (or (:current-time planned-query) current-time)
                                               (expr->value {:args args})
                                               (time/->instant {:default-tz default-tz}))
                                       (expr/current-time))]

                  (when (seq (:warnings planned-query))
                    (.increment query-warning-counter))

                  (.acquire ref-ctr)
                  (try
                    (binding [expr/*clock* (InstantSource/fixed current-time)
                              expr/*default-tz* default-tz

                              ;; both snapshot-token and snapshot-time form upper bounds - the result is the intersection of the two
                              expr/*snapshot-token* (some-> (cond-> (or (some-> (:snapshot-token planned-query snapshot-token)
                                                                                (expr->value {:args args})
                                                                                (basis/<-time-basis-str)
                                                                                (doto (validate-basis-not-before snaps)))

                                                                        (default-basis snaps))
                                                              snapshot-time (basis/cap-basis (time/->instant snapshot-time)))
                                                            (basis/->time-basis-str))
                              expr/*await-token* await-token]

                      (if (:explain? planned-query)
                        (let [explain-plan (->explain-plan emitted-query)]
                          (-> (PagesCursor. allocator nil [explain-plan])
                              (wrap-result-fields (explain-plan-fields explain-plan))
                              (wrap-closeables ref-ctr (cond-> [snaps allocator]
                                                         close-args? (cons args)))))

                        (let [cursor (-> (->cursor {:allocator allocator,
                                                    :snaps snaps
                                                    :snapshot-token expr/*snapshot-token*
                                                    :current-time current-time
                                                    :args args, :schema table-info
                                                    :default-tz default-tz
                                                    :explain-analyze? (:explain-analyze? planned-query)})

                                         (wrap-result-fields fields)
                                         (wrap-dynvars current-time expr/*snapshot-token* default-tz ref-ctr))]
                          (if (:explain-analyze? planned-query)
                            (try
                              (.forEachRemaining cursor (fn [_]))
                              (-> (PagesCursor. allocator nil [(explain-analyze-results cursor)])
                                  (wrap-result-fields explain-analyze-fields)
                                  (wrap-closeables ref-ctr (cond->> [snaps allocator]
                                                             close-args? (cons args))))
                              (finally
                                (util/close cursor)))

                            (-> cursor
                                (wrap-closeables ref-ctr (cond->> [snaps allocator]
                                                           close-args? (cons args))))))))

                    (catch Throwable t
                      (.release ref-ctr)
                      (when close-args?
                        (util/close args))
                      (throw t)))))))))))

  AutoCloseable
  (close [_]
    (when-not (.tryClose ref-ctr (Duration/ofMinutes 1))
      (log/warn "Failed to shut down after 60s due to outstanding queries"))

    ;; Clear the plan cache itself
    (.invalidateAll plan-cache)
    (.cleanUp plan-cache)

    (util/close allocator)))

(defmethod ig/prep-key ::query-source [_ opts]
  (merge opts
         {:allocator (ig/ref :xtdb/allocator)
          :metrics-registry (ig/ref :xtdb.metrics/registry)
          :scan-emitter (ig/ref :xtdb.operator.scan/scan-emitter)}))

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
