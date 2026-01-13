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
            [xtdb.sql :as sql]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import clojure.lang.MapEntry
           (com.github.benmanes.caffeine.cache Cache Caffeine)
           io.micrometer.core.instrument.Counter
           java.lang.AutoCloseable
           (java.time Duration InstantSource)
           (java.util HashMap LinkedHashMap)
           [java.util.concurrent.atomic AtomicBoolean]
           (java.util.function Function)
           [java.util.stream Stream StreamSupport]
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           org.apache.arrow.vector.types.pojo.Field
           (xtdb ICursor IResultCursor IResultCursor$ErrorTrackingCursor PagesCursor)
           (xtdb.antlr Sql$DirectlyExecutableStatementContext)
           (xtdb.api.query IKeyFn)
           (xtdb.arrow RelationReader VectorReader VectorType)
           (xtdb.database Database$Catalog)
           (xtdb.indexer Snapshot)
           xtdb.operator.scan.IScanEmitter
           (xtdb.query IQuerySource PreparedQuery)
           xtdb.util.RefCounter))

(defn- wrap-result-types [^ICursor cursor, result-types]
  (reify IResultCursor
    (getResultTypes [_] result-types)
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
    (getResultTypes [_] (.getResultTypes cursor))
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
      (getResultTypes [_] (.getResultTypes cursor))
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

(defn- ->result-types [ordered-outer-projection vec-types]
  (let [result (LinkedHashMap.)]
    (if ordered-outer-projection
      (doseq [col-name ordered-outer-projection]
        (.put result (str col-name) (get vec-types col-name)))
      (doseq [[col-name vec-type] vec-types]
        (.put result (str col-name) vec-type)))
    result))

(defn ->arg-types [^RelationReader args]
  (->> args
       (into {} (map (fn [^VectorReader col]
                       (MapEntry/create (symbol (.getName col)) (.getType col)))))))

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

    :else (throw (err/incorrect :unknown-query-type "Unknown query type"
                                {:query query, :type (type query)}))))

(defn- plan-query [parsed-query query-opts]
  (cond
    (vector? parsed-query) parsed-query
    (instance? Sql$DirectlyExecutableStatementContext parsed-query) (sql/plan parsed-query query-opts)

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
                   param-types, {:keys [default-tz]}]
  (.get emit-cache {:scan-vec-types (scan/scan-vec-types db-cat snaps scan-cols)

                    ;; this one is just to reset the cache for up-to-date stats
                    ;; probably over-zealous
                    :last-known-blocks (->> (for [db-name (.getDatabaseNames db-cat)]
                                              [db-name (some-> (.databaseOrNull db-cat db-name) .getBlockCatalog .getCurrentBlockIndex)]))

                    :default-tz default-tz
                    :param-types param-types
                    :explain-analyze? explain-analyze?}

        (fn [{:keys [scan-vec-types param-types default-tz]}]
          (binding [expr/*default-tz* default-tz]
            (-> (lp/emit-expr conformed-plan
                              {:scan-vec-types scan-vec-types
                               :default-tz default-tz
                               :param-types param-types
                               :db-cat db-cat
                               :scan-emitter scan-emitter})
                (update :vec-types (fn [vts]
                                     (->result-types col-names vts))))))))

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

(defn- explain-plan-types [explain-plan]
  (let [^VectorType vec-type (->> (map types/value->vec-type explain-plan)
                                  (apply types/merge-types))
        children (.getChildren vec-type)]
    (LinkedHashMap. ^java.util.Map
                    (into {} (for [col-name '[depth op explain]]
                               [(str col-name) (get children (str col-name))])))))

(def ^:private explain-analyze-types
  (LinkedHashMap. ^java.util.Map
                  {"depth" (types/->type :utf8)
                   "op" (types/->type :keyword)
                   "total_time" (types/->type [:duration :micro])
                   "time_to_first_page" (types/->type [:duration :micro])
                   "page_count" (types/->type :i64)
                   "row_count" (types/->type :i64)}))

(defn- explain-analyze-results [^IResultCursor cursor]
  (letfn [(->results [^ICursor cursor, depth]
            (lazy-seq
             (if-let [ea (.getExplainAnalyze cursor)]
               (cons {:depth (str (str/join (repeat depth "  ")) "->")
                      :op (keyword (.getCursorType cursor))
                      :total-time (.getTotalTime ea)
                      :time-to-first-page (.getTimeToFirstPage ea)
                      :page-count (.getPageCount ea)
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
                                                       (into {} (map (fn [^Field f]
                                                                       [(symbol (.getName f)) (types/->type f)]))))
                                                  query-opts)
                        vec-types (cond
                                    (:explain? planned-query) (explain-plan-types (->explain-plan emitted-query))
                                    (:explain-analyze? planned-query) explain-analyze-types
                                    :else (:vec-types emitted-query))]
                    (mapv (fn [[col-name ^VectorType vec-type]]
                            (.toField vec-type col-name))
                          vec-types)))))

            (getWarnings [_] (:warnings (plan-query* @!table-info)))

            (openQuery [_ {:keys [args current-time snapshot-token snapshot-time default-tz close-args? await-token tracer query-text]
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

                      {:keys [vec-types ->cursor] :as emitted-query} (emit-query planned-query scan-emitter db-cat snaps (->arg-types args) query-opts)
                      current-time (or (some-> (or (:current-time planned-query) current-time)
                                               (expr->value {:args args})
                                               (time/->instant {:default-tz default-tz}))
                                       (expr/current-time))
                      query-span (when tracer
                                   (metrics/start-span tracer "xtdb.query" {:attributes {:query.text query-text}}))
                      closeable-query-span (when query-span
                                             (reify AutoCloseable
                                               (close [_] (.end query-span))))]

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
                              (wrap-result-types (explain-plan-types explain-plan))
                              (wrap-closeables ref-ctr (cond->> [snaps allocator]
                                                         close-args? (cons args)
                                                         closeable-query-span (cons closeable-query-span)))))

                        (let [result-types (->result-types (:ordered-outer-projection planned-query) vec-types)
                              cursor (-> (->cursor {:allocator allocator,
                                                    :snaps snaps
                                                    :snapshot-token expr/*snapshot-token*
                                                    :current-time current-time
                                                    :args args, :schema table-info
                                                    :default-tz default-tz
                                                    :explain-analyze? (:explain-analyze? planned-query)
                                                    :tracer tracer
                                                    :query-span query-span})

                                         (wrap-result-types result-types)
                                         (wrap-dynvars current-time expr/*snapshot-token* default-tz ref-ctr))]
                          (if (:explain-analyze? planned-query)
                            (try
                              (.forEachRemaining cursor (fn [_]))
                              (-> (PagesCursor. allocator nil [(explain-analyze-results cursor)])
                                  (wrap-result-types explain-analyze-types)
                                  (wrap-closeables ref-ctr (cond->> [snaps allocator]
                                                             close-args? (cons args)
                                                             closeable-query-span (cons closeable-query-span))))
                              (finally
                                (util/close cursor)))

                            (-> cursor
                                (wrap-closeables ref-ctr (cond->> [snaps allocator]
                                                           close-args? (cons args)
                                                           closeable-query-span (cons closeable-query-span))))))))

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

(defmethod ig/expand-key ::query-source [k opts]
  {k (merge opts
            {:allocator (ig/ref :xtdb/allocator)
             :metrics-registry (ig/ref :xtdb.metrics/registry)
             :scan-emitter (ig/ref :xtdb.operator.scan/scan-emitter)})})

;; Estimated 400MB max size given large SLT queries, observed during SLT between/10/ run.
;; with an inferred value of roughly 100KB per parsed query.
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
