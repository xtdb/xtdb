(ns xtdb.test-util
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [cognitect.anomalies :as-alias anom]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.indexer :as idx]
            [xtdb.indexer.live-index :as li]
            [xtdb.log :as xt-log]
            [xtdb.logical-plan :as lp]
            [xtdb.node :as xtn]
            [xtdb.protocols :as xtp]
            [xtdb.query :as q]
            [xtdb.serde :as serde]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang ExceptionInfo)
           java.net.ServerSocket
           (java.nio.file Files Path)
           java.nio.file.attribute.FileAttribute
           (java.time Duration Instant InstantSource LocalTime Period YearMonth ZoneId ZoneOffset)
           (java.time.temporal ChronoUnit)
           (java.util LinkedList List)
           (java.util.function IntConsumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (org.apache.arrow.vector FieldVector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo Field Schema)
           (xtdb BufferPool ICursor)
           (xtdb.api TransactionKey)
           xtdb.api.query.IKeyFn
           (xtdb.arrow Relation RelationReader Vector VectorReader)
           (xtdb.indexer LiveTable Watermark Watermark$Source)
           (xtdb.log.proto TemporalMetadata TemporalMetadata$Builder)
           (xtdb.query IQuerySource PreparedQuery)
           (xtdb.trie MetadataFileWriter)
           xtdb.types.ZonedDateTimeRange
           (xtdb.util RefCounter RowCounter TemporalBounds TemporalDimension)))

#_{:clj-kondo/ignore [:uninitialized-var]}
(def ^:dynamic ^org.apache.arrow.memory.BufferAllocator *allocator*)

(defn with-allocator [f]
  (util/with-open [allocator (RootAllocator.)]
    (binding [*allocator* allocator]
      (f))))

(t/deftest test-memory-leak-doesnt-mask-original-error
  (t/is (thrown? ExceptionInfo
                 (with-allocator
                   (fn []
                     (.buffer *allocator* 10)
                     (throw (ex-info "boom!" {})))))))

(def ^:dynamic *node-opts* {})

#_{:clj-kondo/ignore [:uninitialized-var]}
(def ^:dynamic ^xtdb.api.Xtdb *node*)

(defn with-opts
  ([opts] (partial with-opts opts))
  ([opts f]
   (binding [*node-opts* (merge *node-opts* opts)]
     (f))))

(declare component)

(defn with-node [f]
  (util/with-open [node (xtn/start-node *node-opts*)]
    (binding [*node* node]
      (f))))

(defn free-port ^long []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

#_{:clj-kondo/ignore [:uninitialized-var]}
(def ^:dynamic *node-type*)

(defn with-each-api-implementation [api-implementations]
  (fn [f]
    (doseq [[node-type run-tests] api-implementations]
      (binding [*node-type* node-type]
        (t/testing (str node-type)
          (run-tests f))))))

(defn component
  ([k] (component *node* k))
  ([node k] (util/component node k)))

(defn latest-completed-tx ^TransactionKey [node]
  (:latest-completed-tx (xtp/status node)))

(defn latest-submitted-tx-id ^TransactionKey [node]
  (xtp/latest-submitted-tx-id node))

;; TODO inline this now that we have `log/await-tx`
(defn then-await-tx
  (^TransactionKey [node]
   (xt-log/await-tx node))

  (^TransactionKey [tx-id node]
   (xt-log/await-tx node tx-id))

  (^TransactionKey [tx-id node timeout]
   (xt-log/await-tx node tx-id timeout)))

(defn ->instants
  ([u] (->instants u 1))
  ([u len] (->instants u len #inst "2020-01-01"))
  ([u ^long len inst-like]
   (let [inst (time/->instant inst-like)
         zdt (.atZone inst (ZoneId/of "UTC"))
         year (.getYear zdt)
         month (.getValue (.getMonth zdt))]
     (letfn [(to-seq [^ChronoUnit unit]
               (->> (iterate #(.plus ^YearMonth % len unit) (YearMonth/of year month))
                    (map #(Instant/ofEpochSecond (.toEpochSecond (.atDay ^YearMonth % 1) LocalTime/MIDNIGHT ZoneOffset/UTC)))))]
       (case u
         :second (iterate #(.plus ^Instant % (Duration/ofSeconds len)) inst)
         :minute (iterate #(.plus ^Instant % (Duration/ofMinutes len)) inst)
         :hour (iterate #(.plus ^Instant % (Duration/ofHours len)) inst)
         :day (iterate #(.plus ^Instant % (Period/ofDays len)) inst)
         :month (to-seq ChronoUnit/MONTHS)
         :quarter (->> (iterate #(.plusMonths ^YearMonth % (* 3 len)) (YearMonth/of year month))
                       (map #(Instant/ofEpochSecond (.toEpochSecond (.atDay ^YearMonth % 1) LocalTime/MIDNIGHT ZoneOffset/UTC))))
         :year (to-seq ChronoUnit/YEARS))))))

(defn ->mock-clock
  (^java.time.InstantSource []
   (->mock-clock (iterate #(.plus ^Instant % (Period/ofDays 1))
                          (.toInstant #inst "2020-01-01"))))

  (^java.time.InstantSource [^Iterable insts]
   (let [it (.iterator insts)]
     (reify InstantSource
       (instant [_]
         (assert (.hasNext it) "out of insts!")
         (time/->instant (.next it)))))))

(defn with-mock-clock [f]
  (with-opts {:log [:in-memory {:instant-src (->mock-clock)}]} f))

(defn ->tstz-range ^xtdb.types.ZonedDateTimeRange [from to]
  (ZonedDateTimeRange. (time/->zdt from) (some-> to time/->zdt)))

(defn finish-block! [node]
  (then-await-tx node)
  (li/finish-block! node))

(defn open-vec
  (^xtdb.arrow.Vector [^Field field]
   (Vector/fromField *allocator* field))

  (^xtdb.arrow.Vector [col-name-or-field ^List rows]
   (cond
     (string? col-name-or-field) (Vector/fromList *allocator* ^String col-name-or-field rows)
     (instance? Field col-name-or-field) (Vector/fromList *allocator* ^Field col-name-or-field rows)
     :else (throw (err/incorrect ::invalid-vec {:col-name-or-field col-name-or-field})))))

(defn open-rel
  (^xtdb.arrow.Relation [] (vw/open-rel *allocator*))

  (^xtdb.arrow.RelationReader [rows-or-cols]
   (cond
     (and (map? rows-or-cols) (every? sequential? (vals rows-or-cols)))
     (Relation/openFromCols *allocator* rows-or-cols)

     (and (sequential? rows-or-cols) (every? map? rows-or-cols))
     (Relation/openFromRows *allocator* rows-or-cols)

     :else (throw (err/incorrect ::invalid-rel {:rows-or-cols rows-or-cols})))))

(defn open-args ^xtdb.arrow.RelationReader [args]
  (vw/open-args *allocator* args))

(defn populate-root ^org.apache.arrow.vector.VectorSchemaRoot [^VectorSchemaRoot root rows]
  (.clear root)

  (let [field-vecs (.getFieldVectors root)
        row-count (count rows)]
    (doseq [^FieldVector field-vec field-vecs]
      (vw/write-vec! field-vec (map (keyword (.getName (.getField field-vec))) rows)))

    (.setRowCount root row-count)
    root))

(defn ->cursor
  (^xtdb.ICursor [^Schema schema, pages] (->cursor *allocator* schema pages))

  (^xtdb.ICursor [^BufferAllocator allocator ^Schema schema, pages]
   (let [pages (LinkedList. pages)
         root (VectorSchemaRoot/create schema allocator)]
     (reify ICursor
       (tryAdvance [_ c]
         (if-let [page (some-> (.poll pages) vec)]
           (do
             (populate-root root page)
             (.accept c (vr/<-root root))
             true)
           false))

       (close [_]
         (.close root))))))

(defmethod lp/ra-expr ::pages [_]
  (s/cat :op #{::pages}
         :col-types (s/? (s/map-of simple-symbol? some?))
         :pages vector?))

(defmethod lp/emit-expr ::pages [{:keys [col-types pages stats]} _args]
  (let [fields (or (some->> col-types (into {} (map (fn [[col-name col-type]]
                                                      [col-name (types/col-type->field col-name col-type)]))))
                   (vw/rows->fields (into [] cat pages)))
        ^Schema schema (Schema. (for [[col-name field] fields]
                                  (types/field-with-name field (str col-name))))]
    {:fields fields
     :stats stats
     :->cursor (fn [{:keys [allocator]}]
                 (->cursor allocator schema pages))}))

(defn <-cursor
  ([^ICursor cursor] (<-cursor cursor #xt/key-fn :kebab-case-keyword))
  ([^ICursor cursor ^IKeyFn key-fn]
   (let [!res (volatile! (transient []))]
     (.forEachRemaining cursor
                        (fn [^RelationReader rel]
                          (vswap! !res conj! (.toMaps rel key-fn))))
     (persistent! @!res))))

(defn query-ra
  ([query] (query-ra query {}))
  ([query {:keys [node args preserve-pages? with-col-types? key-fn] :as query-opts
           :or {key-fn (serde/read-key-fn :kebab-case-keyword)}}]
   (let [{:keys [live-idx]} node
         allocator (:allocator node *allocator*)
         query-opts (-> query-opts
                        (cond-> node (-> (update :after-tx-id (fnil identity (xtp/latest-submitted-tx-id node)))
                                         (doto (-> :after-tx-id (then-await-tx node))))))

         ^PreparedQuery pq (if node
                             (let [^IQuerySource q-src (util/component node ::q/query-source)]
                               (.prepareRaQuery q-src query live-idx query-opts))
                             (q/prepare-ra query {:allocator allocator
                                                  :ref-ctr (RefCounter.)
                                                  :wm-src (reify Watermark$Source
                                                            (openWatermark [_]
                                                              (Watermark. nil nil {})))}))]
     (util/with-open [^RelationReader args-rel (if args
                                                 (vw/open-args allocator args)
                                                 vw/empty-args)
                      res (.openQuery pq (-> (select-keys query-opts [:snapshot-time :current-time :after-tx-id :table-args :default-tz])
                                             (assoc :args args-rel, :close-args? false)))]
       (let [rows (-> (<-cursor res (serde/read-key-fn key-fn))
                      (cond->> (not preserve-pages?) (into [] cat)))]
         (if with-col-types?
           {:res rows,
            :col-types (->> (.getResultFields res)
                            (into {} (map (juxt #(symbol (.getName ^Field %))
                                                types/field->col-type))))}
           rows))))))

(t/deftest round-trip-cursor
  (with-allocator
    (fn []
      (let [pages [[{:name "foo", :age 20}
                    {:name "bar", :age 25}]
                   [{:name "baz", :age 30}]]]
        (with-open [cursor (->cursor (Schema. [(types/col-type->field "name" :utf8)
                                               (types/col-type->field "age" :i64)])
                                     pages)]

          (t/is (= pages (<-cursor cursor))))))))

(defn ->local-node ^xtdb.api.Xtdb [{:keys [^Path node-dir ^String buffers-dir
                                           rows-per-block log-limit page-limit instant-src
                                           compactor-threads healthz-port gc? blocks-to-keep garbage-lifetime
                                           instant-source-for-non-tx-msgs?]
                                    :or {buffers-dir "objects" healthz-port 8080 instant-source-for-non-tx-msgs? false}}]
  (let [instant-src (or instant-src (->mock-clock))
        healthz-port (if (util/port-free? healthz-port) healthz-port (util/free-port))]
    (xtn/start-node (cond-> {:healthz {:port healthz-port}
                             :log [:local {:path (.resolve node-dir "log"), :instant-src instant-src
                                           :instant-source-for-non-tx-msgs? instant-source-for-non-tx-msgs?}]
                             :storage [:local {:path (.resolve node-dir buffers-dir)}]
                             :indexer (->> {:log-limit log-limit, :page-limit page-limit, :rows-per-block rows-per-block}
                                           (into {} (filter val)))
                             :compactor (->> {:threads compactor-threads}
                                             (into {} (filter val)))}
                      (not (nil? gc?)) (assoc :garbage-collector
                                              (cond-> {:enabled? gc?}
                                                blocks-to-keep (assoc :blocks-to-keep blocks-to-keep)
                                                garbage-lifetime (assoc :garbage-lifetime garbage-lifetime)))))))

(defn with-tmp-dir* [prefix f]
  (let [dir (Files/createTempDirectory prefix (make-array FileAttribute 0))]
    (try
      (f dir)
      (finally
        (util/delete-dir dir)))))

(defmacro with-tmp-dirs
  "Usage:
    (with-tmp-dirs #{log-dir objects-dir}
      ...)"
  [[dir-binding & more-bindings] & body]
  (if dir-binding
    `(with-tmp-dir* ~(name dir-binding)
       (fn [~(vary-meta dir-binding assoc :tag 'java.nio.file.Path)]
         (with-tmp-dirs #{~@more-bindings}
           ~@body)))
    `(do ~@body)))

(defn ->temporal-bounds
  ([min max] (->temporal-bounds min max min))
  ([vf-min vt-max sf-min] (->temporal-bounds vf-min vt-max sf-min Long/MAX_VALUE))
  ([vf-min vt-max sf-min st-max] (TemporalBounds. (TemporalDimension. vf-min vt-max) (TemporalDimension. sf-min st-max))))

(defn ->temporal-metadata
  ([min max] (->temporal-metadata min max min))
  ([vf-min vt-max sf-min] (->temporal-metadata vf-min vt-max sf-min sf-min))
  ([vf-min vt-max sf-min sf-max]
   (let [^TemporalMetadata$Builder builder (TemporalMetadata/newBuilder)]
     (.setMinValidFrom builder vf-min)
     (.setMaxValidFrom builder vf-min)
     (.setMinValidTo builder vt-max)
     (.setMaxValidTo builder vt-max)
     (.setMinSystemFrom builder sf-min)
     (.setMaxSystemFrom builder sf-max)
     (.build builder))))

(defn open-arrow-hash-trie-rel ^xtdb.arrow.Relation [^BufferAllocator al, paths]
  (util/with-close-on-catch [meta-rel (Relation/open al MetadataFileWriter/metaRelSchema)]
    (let [nodes-wtr (.vectorFor meta-rel "nodes")
          nil-wtr (.vectorFor nodes-wtr "nil")
          iid-branch-wtr (.vectorFor nodes-wtr "branch-iid")
          iid-branch-el-wtr (.getListElements iid-branch-wtr)

          data-wtr (.vectorFor nodes-wtr "leaf")
          data-page-idx-wtr (.vectorFor data-wtr "data-page-idx")
          metadata-wtr (.vectorFor data-wtr "columns")]
      (letfn [(write-paths [paths]
                (cond
                  (nil? paths) (.writeNull nil-wtr)

                  (number? paths) (do
                                    (.writeInt data-page-idx-wtr paths)
                                    (.endList metadata-wtr)
                                    (.endStruct data-wtr))

                  (vector? paths) (let [!page-idxs (IntStream/builder)]
                                    (doseq [child paths]
                                      (.add !page-idxs (if child
                                                         (do
                                                           (write-paths child)
                                                           (dec (.getRowCount meta-rel)))
                                                         -1)))
                                    (.forEach (.build !page-idxs)
                                              (reify IntConsumer
                                                (accept [_ idx]
                                                  (if (= idx -1)
                                                    (.writeNull iid-branch-el-wtr)
                                                    (.writeInt iid-branch-el-wtr idx)))))
                                    (.endList iid-branch-wtr)))

                (.endRow meta-rel))]
        (write-paths paths)))

    meta-rel))

(defn open-live-table ^xtdb.indexer.LiveTable [table-name]
  (LiveTable. *allocator* BufferPool/UNUSED table-name (RowCounter. 0)))

(defn index-tx! [^LiveTable live-table, ^TransactionKey tx-key, docs]
  (let [system-time (.getSystemTime tx-key)
        live-table-tx (.startTx live-table tx-key true)]
    (try
      (let [doc-wtr (.getDocWriter live-table-tx)]
        (doseq [{eid :xt/id, :as doc} docs
                :let [{:keys [:xt/valid-from :xt/valid-to],
                       :or {valid-from system-time, valid-to (time/micros->instant Long/MAX_VALUE)}} (meta doc)]]
          (.logPut live-table-tx (util/->iid eid)
                   (time/instant->micros valid-from) (time/instant->micros valid-to)
                   (fn [] (.writeObject doc-wtr doc)))))
      (catch Throwable t
        (.abort live-table-tx)
        (throw t)))

    (.commit live-table-tx)))

(defn byte-buffer->path [^java.nio.ByteBuffer bb]
  (mapcat (fn [b]
            [(bit-and (bit-shift-right b 6) 3)
             (bit-and (bit-shift-right b 4) 3)
             (bit-and (bit-shift-right b 2) 3)
             (bit-and b 3)])
          (.array bb)))

(defn uuid-seq [n]
  (letfn [(new-uuid [n]
            (java.util.UUID. (Long/reverse n) 0))]
    (map new-uuid (range n))))

(defn vec->vals
  ([^VectorReader rdr] (vec->vals rdr #xt/key-fn :kebab-case-keyword))
  ([^VectorReader rdr ^IKeyFn key-fn] (.toList rdr key-fn)))

(defn q-sql
  "Like xtdb.api/q, but also returns the result type."
  ([node query] (q-sql node query {}))
  ([node query opts]
   (let [^PreparedQuery prepared-q (xtp/prepare-sql node query opts)]
     {:res (xt/q node query opts)
      :res-type (mapv (juxt #(.getName ^Field %) types/field->col-type) (.getColumnFields prepared-q []))})))
