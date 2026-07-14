(ns xtdb.query
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
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
            [xtdb.sql.parse :as parse-sql]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import clojure.lang.MapEntry
           (com.github.benmanes.caffeine.cache Cache Caffeine)
           io.micrometer.core.instrument.Counter
           java.lang.AutoCloseable
           (java.time Duration InstantSource)
           (java.util HashMap LinkedHashMap Map)
           [java.util.concurrent.atomic AtomicBoolean]
           (java.util.function Function)
           [java.util.stream Stream StreamSupport]
           (org.antlr.v4.runtime.misc Interval)
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           org.apache.arrow.vector.types.pojo.Field
           (xtdb ICursor ResultCursor ResultCursor$ErrorTrackingCursor PagesCursor)
           (xtdb.antlr Sql$DirectlyExecutableStatementContext)
           (xtdb.api.query IKeyFn)
           (xtdb.arrow RelationReader VectorReader VectorType)
           (xtdb.indexer DatabaseSnapshot Snapshot)
           xtdb.NodeBase
           xtdb.operator.scan.IScanEmitter
           (xtdb.query IQuerySource IQuerySource$Factory ParsedStatement PrepareOpts PreparedQuery SqlStatement$Assert SqlStatement$CreateTable SqlStatement$Delete SqlStatement$Erase SqlStatement$GrantRole SqlStatement$Patch SqlStatement$Put SqlStatement$RevokeRole)
           xtdb.util.RefCounter))

(defn- wrap-result-types [^ICursor cursor, result-types]
  (reify ResultCursor
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

(defn- wrap-dynvars ^xtdb.ResultCursor [^ResultCursor cursor
                                         current-time, snapshot-token, default-tz
                                         ^RefCounter ref-ctr]
  (reify ResultCursor
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

(defn- wrap-closeables ^xtdb.ResultCursor [^ResultCursor cursor, ^RefCounter ref-ctr, closeables]
  (let [!closed? (AtomicBoolean. false)]
    (reify ResultCursor
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
  (doseq [[db-name ^DatabaseSnapshot snap] snaps
          [partition-idx ^Snapshot part-snap] (map-indexed vector (.getPartitions snap))]
    (let [snap-tx (.getTxBasis part-snap)
          system-time (get-in basis [db-name partition-idx])]
      (when (and system-time
                 (or (nil? snap-tx)
                     (neg? (compare (.getSystemTime snap-tx) system-time))))
        (throw (err/incorrect :xtdb/unindexed-tx
                              (format "system-time (%s) is after the latest completed tx (%s)"
                                      (str system-time) (pr-str snap-tx))
                              {:latest-completed-tx snap-tx
                               :system-time system-time}))))))

(defn- default-basis [snaps]
  (->> (for [[db-name ^DatabaseSnapshot snap] snaps
             :let [sys-times (->> (.getPartitions snap)
                                  (mapv (fn [^Snapshot part-snap]
                                          (some-> (.getTxBasis part-snap) (.getSystemTime)))))]
             :when (some identity sys-times)]
         [db-name sys-times])
       (into {})
       (not-empty)))

(defn- plan-query [parsed-query query-opts]
  (cond
    (vector? parsed-query) (with-meta parsed-query (select-keys query-opts [:explain? :explain-analyze?]))
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
                   scan-emitter, db-cat, snaps
                   param-types, {:keys [default-tz]}]
  (.get emit-cache {:scan-vec-types (scan/scan-vec-types db-cat snaps scan-cols)

                    ;; this one is just to reset the cache for up-to-date stats
                    ;; probably over-zealous
                    :last-known-blocks (->> (for [db-name (.getDatabaseNames db-cat)]
                                              [db-name (some-> (.databaseOrNull db-cat db-name) .getQueryState .getBlockCatalog .getCurrentBlockIndex)]))

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
        ;; asMono because it's a struct
        children (.getChildren (.asMono vec-type))]
    (LinkedHashMap. ^Map (into {} (for [col-name '[depth op explain]]
                                    [(str col-name) (get children (str col-name))])))))

(def ^:private explain-analyze-types
  (LinkedHashMap. ^Map (identity {"depth" #xt/type :utf8
                                  "op" #xt/type :keyword
                                  ;; struct (not transit) so it renders as JSON over pgwire — readable in Metabase
                                  "attributes" #xt/type [:struct {"scan_db" :utf8, "scan_source" :utf8
                                                                  "scan_files_pruned" :i64, "scan_files_used" :i64
                                                                  "scan_pages_pruned" :i64, "scan_pages_used" :i64
                                                                  "scan_rows_read" :i64}]
                                  "total_time" #xt/type [:duration :micro]
                                  "time_to_first_page" #xt/type [:duration :micro]
                                  "page_count" #xt/type :i64
                                  "row_count" #xt/type :i64
                                  "pushdowns" #xt/type :transit})))

(defn- truncate-pushdown-val [k v]
  (case k
    :bloom-filter {:cardinality (.getCardinality ^org.roaringbitmap.buffer.MutableRoaringBitmap v)}
    :iids {:count (.size ^java.util.Collection v)}
    v))

(defn- truncate-pushdowns
  "Formats pushdowns for readable explain-analyze output.
   Replaces bloom filters with their cardinality, and large IID sets with a count."
  [pushdowns]
  (vec (for [[col-name col-pushdowns] pushdowns]
         (into {:column col-name}
               (map (fn [[k v]] [k (truncate-pushdown-val k v)]))
               col-pushdowns))))

(defn- explain-analyze-results [^ResultCursor cursor]
  (letfn [(->results [^ICursor cursor, depth]
            (lazy-seq
             (if-let [ea (.getExplainAnalyze cursor)]
               (cons (let [attrs (.getCursorAttributes ea)]
                       {:depth (str (str/join (repeat depth "  ")) "->")
                        :op (keyword (.getCursorType cursor))
                        :attributes (not-empty (into {} attrs))
                        :total-time (.getTotalTime ea)
                        :time-to-first-page (.getTimeToFirstPage ea)
                        :page-count (.getPageCount ea)
                        :row-count (.getRowCount ea)
                        :pushdowns (not-empty (truncate-pushdowns (.getPushdowns ea)))})
                     (->> (for [child (.getChildCursors cursor)]
                            (->results child (inc depth)))
                          (sequence cat)))
               (->results (first (.getChildCursors cursor)) depth))))]

    (vec (->results cursor 0))))

(defn- ast-source-text [^Sql$DirectlyExecutableStatementContext ast]
  (let [start (.getStart ast)]
    (.getText (.getInputStream start)
              (Interval/of (.getStartIndex start) (.getStopIndex (.getStop ast))))))

(defn- ->prepare-opts-map
  "Prepare-time opts reach the planner as a Clojure map; translate the typed [PrepareOpts] here, at the
  planner's edge."
  [^PrepareOpts opts]
  (cond-> {}
    (.getDefaultTz opts) (assoc :default-tz (.getDefaultTz opts))
    (.getDefaultDb opts) (assoc :default-db (.getDefaultDb opts))
    (.getCurrentTime opts) (assoc :current-time (.getCurrentTime opts))
    (.getArgFields opts) (assoc :arg-fields (.getArgFields opts))
    (.getExplain opts) (assoc :explain? true)
    (.getExplainAnalyze opts) (assoc :explain-analyze? true)))

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
                                       (select-keys [:default-db :db-names :decorrelate? :explain? :explain-analyze? :arg-fields])
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
    (.prepareRa this query db-cat query-opts))

  (prepareRa [this parsed-query db-cat query-opts]
    (let [^ParsedStatement stmt (when (instance? ParsedStatement parsed-query) parsed-query)
          parsed-query (if stmt (.getAst stmt) parsed-query)
          qtext (cond stmt (.getOriginalSql stmt)
                      (instance? Sql$DirectlyExecutableStatementContext parsed-query) (ast-source-text parsed-query))
          {:keys [default-tz query-text] :as query-opts} (-> (->prepare-opts-map query-opts)
                                                              (update :default-tz (fnil identity expr/*default-tz*))
                                                              (cond-> qtext (assoc :query-text qtext))
                                                              (assoc :db-names (vec (sort (.getDatabaseNames db-cat)))))]
      (letfn [(open-snaps []
                ;; TODO this opens up a snapshot for *every* db in the catalog
                ;; when people have 'proper' multi-tenancy, this will be a problem
                ;; thankfully we can probably figure out what databases may be relevant to the query at parse-time
                (util/with-close-on-catch [!snaps (HashMap.)]
                  (doseq [db-name (.getDatabaseNames db-cat)]
                    (.put !snaps db-name (.openSnapshot (.databaseOrNull db-cat db-name))))
                  (into {} !snaps)))

              (->table-info []
                ;; TODO this, too, gets the schema for *every* db in the catalog
                (->> (.getDatabaseNames db-cat)
                     (into {} (mapcat (fn [db-name]
                                        (util/with-open [^DatabaseSnapshot snap (.openSnapshot (.databaseOrNull db-cat db-name))]
                                          (->> (.tableInfo snap)
                                               (map (fn [[table-ref cols]] [[db-name table-ref] (set cols)])))))))))

              (plan-query* [table-info]
                (-plan-query this parsed-query query-opts table-info))]

        (let [!table-info (atom (->table-info))]

          (reify PreparedQuery
            (getParamCount [_] (:param-count (plan-query* @!table-info)))
            ;; the plan carries these as symbols; the PreparedQuery contract is List<String>
            (getColumnNames [_] (mapv str (:col-names (plan-query* @!table-info))))
            (getParsed [_] stmt)

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

            (openQuery [_ args opts]
              (let [default-tz (or (.getDefaultTz opts) default-tz)]
              ;; we own the args from here: closed on any failure below, else handed to the cursor (which
              ;; closes them on close). the caller must not close them itself.
              (util/with-close-on-catch [^RelationReader args (or args vw/empty-args)
                                         ^BufferAllocator allocator (if allocator
                                                                      (util/->child-allocator allocator "BoundQuery/openCursor")
                                                                      (RootAllocator.))
                                         snaps (open-snaps)]
                (let [query-opts (-> query-opts (assoc :default-tz default-tz))
                      table-info (reset! !table-info (->table-info))
                      planned-query (plan-query* table-info)

                      {:keys [vec-types ->cursor] :as emitted-query} (emit-query planned-query scan-emitter db-cat snaps (->arg-types args) query-opts)
                      current-time (or (some-> (or (:current-time planned-query) (.getCurrentTime opts))
                                               (expr->value {:args args})
                                               (time/->instant {:default-tz default-tz}))
                                       (expr/current-time))
                      tracer (.getTracer opts)
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
                              expr/*snapshot-token* (some-> (cond-> (or (some-> (:snapshot-token planned-query (.getSnapshotToken opts))
                                                                                (expr->value {:args args})
                                                                                (basis/<-time-basis-str)
                                                                                (doto (validate-basis-not-before snaps)))

                                                                        (default-basis snaps))
                                                              (.getSnapshotTime opts) (basis/cap-basis (time/->instant (.getSnapshotTime opts))))
                                                            (basis/->time-basis-str))]

                      (if (:explain? planned-query)
                        (let [explain-plan (->explain-plan emitted-query)]
                          (-> (PagesCursor. allocator nil [explain-plan])
                              (wrap-result-types (explain-plan-types explain-plan))
                              (wrap-closeables ref-ctr [closeable-query-span args snaps allocator])))

                        (let [result-types (->result-types (:ordered-outer-projection planned-query) vec-types)
                              cursor (-> (->cursor {:allocator allocator,
                                                    :query-source this
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
                                  (wrap-closeables ref-ctr [closeable-query-span args snaps allocator]))
                              (finally
                                (util/close cursor)))

                            (-> cursor
                                (wrap-closeables ref-ctr [closeable-query-span args snaps allocator]))))))

                    (catch Throwable t
                      (.release ref-ctr)
                      (throw t))))))))))))

  (prepareTxSql [this sql db-cat opts]
    (let [[q-tag {:keys [stmt table message user role col-names]}]
          (parse-sql/parse-statement sql {:default-db (.getDefaultDb opts)})]
      (case q-tag
        :grant-role (SqlStatement$GrantRole. user role)
        :revoke-role (SqlStatement$RevokeRole. user role)
        :create-table (SqlStatement$CreateTable. table (or col-names []))

        (let [pq (.prepareRa this stmt db-cat opts)]
          (case q-tag
            (:insert :update) (SqlStatement$Put. pq table)
            :patch (SqlStatement$Patch. pq table)
            :delete (SqlStatement$Delete. pq table)
            :erase (SqlStatement$Erase. pq table)
            :assert (SqlStatement$Assert. pq message)
            (throw (err/incorrect ::invalid-sql-tx-op
                                  "Invalid SQL query sent as transaction operation"
                                  {:query sql})))))))

  (preparePatchDocsQuery [this table valid-from valid-to db-cat opts]
    (let [default-db (.getDefaultDb opts)
          table-info (-> (util/with-open [^DatabaseSnapshot snap (.openSnapshot (.databaseOrNull db-cat default-db))]
                           (.tableInfo snap))
                         (sql/xform-table-info [default-db] default-db))
          plan (-> (sql/plan-patch {:table-info table-info}
                                   {:db-name default-db
                                    :table table
                                    :valid-from valid-from
                                    :valid-to valid-to
                                    :patch-rel (sql/->QueryExpr '[:table {:output-cols [_iid doc]
                                                                          :param ?patch_docs}]
                                                                '[_iid doc])})
                   lp/rewrite-plan)]
      (.prepareRa this plan db-cat opts)))

  AutoCloseable
  (close [_]
    (when-not (.tryClose ref-ctr (Duration/ofMinutes 1))
      (log/warn "Failed to shut down after 60s due to outstanding queries"))

    ;; Clear the plan cache itself
    (.invalidateAll plan-cache)
    (.cleanUp plan-cache)

    (util/close allocator)))

;; Estimated 400MB max size given large SLT queries, observed during SLT between/10/ run.
;; with an inferred value of roughly 100KB per parsed query.
(defn ->query-source [{:keys [^NodeBase base allocator metrics-registry] :as deps}]
  (let [allocator (util/->child-allocator (if base (.getAllocator base) allocator) "query-source")
        metrics-registry (if base (.getMeterRegistry base) metrics-registry)
        ref-ctr (RefCounter.)
        deps (-> deps
                 (dissoc :base)
                 (assoc :ref-ctr ref-ctr
                        :allocator allocator
                        :metrics-registry metrics-registry
                        :query-warning-counter (some-> metrics-registry (metrics/add-counter "query.warning"))
                        :plan-cache (-> (Caffeine/newBuilder)
                                        (.maximumSize 4096)
                                        (.build))))]

    (map->QuerySource deps)))

(defn ->factory ^xtdb.query.IQuerySource$Factory []
  (reify IQuerySource$Factory
    (create [_ allocator meter-registry scan-emitter]
      (->query-source {:allocator allocator
                       :metrics-registry meter-registry
                       :scan-emitter scan-emitter}))))

(defn- cache-key-fn [^IKeyFn key-fn]
  (let [cache (HashMap.)]
    (reify IKeyFn
      (denormalize [_ k]
        (.computeIfAbsent cache k
                          (reify Function
                            (apply [_ k]
                              (.denormalize key-fn k))))))))

(defn cursor->stream
  (^java.util.stream.Stream [^ResultCursor cursor query-opts]
   (cursor->stream cursor query-opts {}))
  (^java.util.stream.Stream [^ResultCursor cursor {:keys [key-fn]} {:keys [query-error-counter]}]
   (let [key-fn (cache-key-fn key-fn)]
     (-> (StreamSupport/stream (cond-> cursor
                                 query-error-counter (ResultCursor$ErrorTrackingCursor. query-error-counter))
                               false)
         ^Stream (.onClose (fn []
                             (util/close cursor)))
         (.flatMap (fn [^RelationReader rel]
                     (.stream (.toMaps rel key-fn))))))))
