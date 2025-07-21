(ns xtdb.operator.scan
  (:require [clojure.spec.alpha :as s]
            [integrant.core :as ig]
            [xtdb.basis :as basis]
            [xtdb.expression :as expr]
            [xtdb.expression.metadata :as expr.meta]
            xtdb.indexer.live-index
            [xtdb.information-schema :as info-schema]
            [xtdb.logical-plan :as lp]
            [xtdb.metadata :as meta]
            xtdb.object-store
            [xtdb.table :as table]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as cat]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang MapEntry)
           java.nio.ByteBuffer
           java.time.Instant
           (java.util LinkedList)
           (java.util.function IntPredicate Predicate)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           [org.apache.arrow.memory.util ArrowBufPointer]
           [org.roaringbitmap.buffer MutableRoaringBitmap]
           (xtdb BufferPool)
           xtdb.arrow.RelationReader
           (xtdb.bloom BloomUtils)
           xtdb.catalog.TableCatalog
           (xtdb.indexer Snapshot Snapshot$Source)
           (xtdb.metadata PageMetadata PageMetadata$Factory)
           (xtdb.operator.scan IidSelector MergePlanPage$Arrow MergePlanPage$Memory RootCache ScanCursor ScanCursor$MergeTask)
           xtdb.table.TableRef
           (xtdb.trie ArrowHashTrie$Leaf HashTrie HashTrieKt MergePlanNode MergePlanTask Trie TrieCatalog)
           (xtdb.util TemporalBounds TemporalDimension)))

(s/def ::table ::table/ref)

;; TODO be good to just specify a single expression here and have the interpreter split it
;; into metadata + col-preds - the former can accept more than just `(and ~@col-preds)
(defmethod lp/ra-expr :scan [_]
  (s/cat :op #{:scan}
         :scan-opts (s/keys :req-un [::table]
                            :opt-un [::lp/for-valid-time ::lp/for-system-time])
         :columns (s/coll-of (s/or :column ::lp/column
                                   :select ::lp/column-expression))))

(definterface IScanEmitter
  (close [])
  (emitScan [^xtdb.database.Database db scan-expr scan-fields param-fields]))

(defn ->scan-cols [{:keys [columns], {:keys [table]} :scan-opts}]
  (for [[col-tag col-arg] columns]
    [table
     (case col-tag
       :column col-arg
       :select (key (first col-arg)))]))

(def ^:dynamic *column->pushdown-bloom* {})

(defn ->temporal-bounds [^BufferAllocator alloc, ^RelationReader args,
                         {:keys [for-valid-time for-system-time]}, ^Instant snapshot-token]
  (letfn [(->time-μs
            ([arg] (->time-μs arg nil))
            ([[tag arg] default]
             (case tag
               :literal (or (some-> arg
                                    (time/sql-temporal->micros expr/*default-tz*))
                            default)
               :param (or (some-> (-> (.vectorForOrNull args (name arg)) (.getObject 0))
                                  (time/sql-temporal->micros expr/*default-tz*))
                          default)
               :now (-> (expr/current-time) (time/instant->micros))
               :expr (let [param-types (expr/->param-types args)
                           projection (expr/->expression-projection-spec "_temporal_expression"
                                                                         (expr/form->expr (list 'cast_tstz arg) {:param-types param-types})
                                                                         {:param-types param-types})]
                       (util/with-open [res (.project projection alloc vw/empty-args {} args)]
                         (if-let [inst-like (.getObject res 0)]
                           (do
                             (time/expect-instant inst-like)
                             (-> inst-like time/->instant time/instant->micros))
                           default))))))
          (apply-constraint [constraint]
            (if-let [[tag & args] constraint]
              (case tag
                :at (let [[at] args
                          at-μs (->time-μs at (-> (expr/current-time) (time/instant->micros)))]
                      (TemporalDimension/at at-μs))

                ;; overlaps [time-from time-to]
                :in (let [[from to] args]
                      ;; TODO asymmetry of defaulting start-of-time-here and end-of-time in TemporalBounds
                      (TemporalDimension/in (->time-μs from time/start-of-time-as-micros)
                                            (some-> to ->time-μs)))

                :between (let [[from to] args]
                           (TemporalDimension/between (->time-μs from time/start-of-time-as-micros)
                                                      (some-> to ->time-μs)))

                :all-time (TemporalDimension.))
              (TemporalDimension.)))]

    (let [^TemporalDimension sys-dim (apply-constraint for-system-time)
          bounds (TemporalBounds. (apply-constraint for-valid-time) sys-dim)]
      ;; we further constrain bases on tx
      (when-let [system-time (some-> snapshot-token time/instant->micros)]
        (.setUpper sys-dim (min (inc system-time) (.getUpper sys-dim)))

        (when-not for-system-time
          (.setLower (.getSystemTime bounds) system-time)))

      bounds)))

(defn tables-with-cols [^Snapshot$Source snap-src]
  (with-open [snap (.openSnapshot snap-src)]
    (.getSchema snap)))

(defn- eid-select->eid [eid-select]
  (cond (= '_id (second eid-select))
        (nth eid-select 2)

        (= '_id (nth eid-select 2))
        (second eid-select)))

(def ^:private dummy-iid (ByteBuffer/allocateDirect 16))

(defn selects->iid-byte-buffer ^ByteBuffer [selects ^RelationReader args-rel]
  (when-let [eid-select (get selects "_id")]
    (when (= '= (first eid-select))
      (when-let [eid (eid-select->eid eid-select)]
        (cond
          (and (s/valid? ::lp/value eid) (util/valid-iid? eid))
          (util/->iid eid)

          (s/valid? ::lp/param eid)
          (let [eid-rdr (.vectorForOrNull args-rel (name eid))]
            (when (= 1 (.getValueCount eid-rdr))
              (let [eid (.getObject eid-rdr 0)]
                (if (util/valid-iid? eid)
                  (util/->iid eid)
                  dummy-iid)))))))))

(defn filter-pushdown-bloom-page-idx-pred ^IntPredicate [^PageMetadata page-metadata ^String col-name]
  (when-let [^MutableRoaringBitmap pushdown-bloom (get *column->pushdown-bloom* (symbol col-name))]
    (let [metadata-rdr (.getMetadataLeafReader page-metadata)
          bloom-rdr (-> (.vectorForOrNull metadata-rdr "columns")
                        (.getListElements)
                        (.vectorFor "bytes")
                        (.vectorFor "bloom"))]
      (reify IntPredicate
        (test [_ page-idx]
          (boolean
            (let [bloom-vec-idx (.rowIndex page-metadata col-name page-idx)]
              (and (>= bloom-vec-idx 0)
                   (or (.isNull bloom-rdr bloom-vec-idx)
                       (MutableRoaringBitmap/intersects pushdown-bloom (BloomUtils/bloomToBitmap bloom-rdr bloom-vec-idx)))))))))))

(defn ->path-pred [^ArrowBuf iid-arrow-buf]
  (when iid-arrow-buf
    (let [iid-ptr (ArrowBufPointer. iid-arrow-buf 0 (.capacity iid-arrow-buf))]
      (reify Predicate
        (test [_ path]
          (zero? (HashTrie/compareToPath iid-ptr path)))))))

(defmethod ig/prep-key ::scan-emitter [_ opts]
  (merge opts
         {:allocator (ig/ref :xtdb/allocator)
          :info-schema (ig/ref :xtdb/information-schema)}))

(defn scan-fields [^TableCatalog table-catalog, ^Snapshot snap scan-cols]
  (letfn [(->field [[table col-name]]
            (let [col-name (str col-name)]
              ;; TODO move to fields here
              (-> (or (some-> (types/temporal-col-types col-name) types/col-type->field)
                      (get-in info-schema/derived-tables [table (symbol col-name)])
                      (get-in info-schema/template-tables [table (symbol col-name)])
                      (types/merge-fields (.getField table-catalog table col-name)
                                          (some-> (.getLiveIndex snap)
                                                  (.liveTable table)
                                                  (.columnField col-name))))
                  (types/field-with-name col-name))))]
    (->> scan-cols
         (into {} (map (juxt identity ->field))))))

(defmethod ig/init-key ::scan-emitter [_ {:keys [^BufferAllocator allocator, info-schema]}]
  (let [table->template-rel+trie (info-schema/table->template-rel+tries allocator)]
    (reify
      IScanEmitter
      (emitScan [_ db {:keys [columns], {:keys [^TableRef table] :as scan-opts} :scan-opts} scan-fields param-fields]
        (let [^PageMetadata$Factory metadata-mgr (.getMetadataManager db)
              ^BufferPool buffer-pool (.getBufferPool db)
              ^TrieCatalog trie-catalog (.getTrieCatalog db)
              ^TableCatalog table-catalog (.getTableCatalog db)
              col-names (->> columns
                             (into #{} (map (fn [[col-type arg]]
                                              (case col-type
                                                :column arg
                                                :select (key (first arg)))))))
              fields (->> col-names
                          (into {} (map (juxt identity
                                              (fn [col-name]
                                                (get scan-fields [table col-name]))))))

              col-names (into #{} (map str) col-names)

              selects (->> (for [[tag arg] columns
                                 :when (= tag :select)
                                 :let [[col-name pred] (first arg)]]
                             (MapEntry/create (str col-name) pred))
                           (into {}))

              col-preds (->> (for [[col-name select-form] selects]
                               ;; for temporal preds, we may not need to re-apply these if they can be represented as a temporal range.
                               (let [input-types {:col-types (update-vals fields types/field->col-type)
                                                  :param-types (update-vals param-fields types/field->col-type)}]
                                 (MapEntry/create col-name
                                                  (expr/->expression-selection-spec (expr/form->expr select-form input-types)
                                                                                    input-types))))
                             (into {}))

              metadata-args (vec (for [[col-name select] selects
                                       :when (not (types/temporal-column? col-name))]
                                   select))

              row-count (.rowCount table-catalog table)]

          {:fields fields
           :stats {:row-count row-count}
           :->cursor (fn [{:keys [allocator, ^Snapshot snapshot, snapshot-token, schema, args]}]
                       (if (and (info-schema/derived-tables table) (not (info-schema/template-tables table)))
                         (let [derived-table-schema (info-schema/derived-tables table)]
                           (info-schema/->cursor info-schema allocator db snapshot derived-table-schema table col-names col-preds schema args))

                         (let [template-table? (info-schema/template-tables table)
                               iid-bb (selects->iid-byte-buffer selects args)
                               col-preds (cond-> col-preds
                                           iid-bb (assoc "_iid" (IidSelector. iid-bb)))
                               metadata-pred (expr.meta/->metadata-selector allocator (cons 'and metadata-args) (update-vals fields types/field->col-type) args)
                               scan-opts (-> scan-opts
                                             (update :for-valid-time
                                                     (fn [fvt]
                                                       (or fvt [:at [:now]]))))
                               live-table-snap (some-> (.getLiveIndex snapshot) (.liveTable table))
                               temporal-bounds (->temporal-bounds allocator args scan-opts (-> (basis/<-time-basis-str snapshot-token)
                                                                                               (get-in ["xtdb" 0])))]
                           (util/with-open [iid-arrow-buf (when iid-bb (util/->arrow-buf-view allocator iid-bb))]
                             (let [merge-tasks (util/with-open [page-metadatas (LinkedList.)]
                                                 (let [segments (cond-> (mapv (fn [{:keys [^String trie-key]}]
                                                                                (let [meta-path (Trie/metaFilePath table trie-key)
                                                                                      page-metadata (.openPageMetadata metadata-mgr meta-path)]
                                                                                  (.add page-metadatas page-metadata)
                                                                                  (into (trie/->Segment (.getTrie page-metadata))
                                                                                        {:data-file-path (Trie/dataFilePath table trie-key)
                                                                                         :page-idx-pred (reduce (fn [^IntPredicate page-idx-pred col-name]
                                                                                                                  (if-let [bloom-page-idx-pred (filter-pushdown-bloom-page-idx-pred page-metadata col-name)]
                                                                                                                    (.and page-idx-pred bloom-page-idx-pred)
                                                                                                                    page-idx-pred))
                                                                                                                (.build metadata-pred page-metadata)
                                                                                                                col-names)
                                                                                         :page-metadata page-metadata})))
                                                                              (-> (cat/trie-state trie-catalog table)
                                                                                  (cat/current-tries)
                                                                                  (cat/filter-tries temporal-bounds)))

                                                                  live-table-snap (conj (-> (trie/->Segment (.getLiveTrie live-table-snap))
                                                                                            (assoc :memory-rel (.getLiveRelation live-table-snap))))
                                                                  template-table? (conj (let [[memory-rel trie] (table->template-rel+trie table)]
                                                                                          (-> (trie/->Segment trie)
                                                                                              (assoc :memory-rel memory-rel)))))]
                                                   (->> (HashTrieKt/toMergePlan segments (->path-pred iid-arrow-buf))
                                                        (into [] (keep (fn [^MergePlanTask mpt]
                                                                         (when-let [leaves (trie/filter-meta-objects
                                                                                            (for [^MergePlanNode mpn (.getMpNodes mpt)
                                                                                                  :let [{:keys [data-file-path page-metadata page-idx-pred trie memory-rel]} (.getSegment mpn)
                                                                                                        node (.getNode mpn)]]
                                                                                              (if data-file-path
                                                                                                (MergePlanPage$Arrow. buffer-pool
                                                                                                                      data-file-path
                                                                                                                      page-idx-pred
                                                                                                                      (.getDataPageIndex ^ArrowHashTrie$Leaf node)
                                                                                                                      page-metadata)
                                                                                                (MergePlanPage$Memory. memory-rel trie node)))
                                                                                            temporal-bounds)]
                                                                           (ScanCursor$MergeTask. leaves (.getPath mpt)))))))))]

                               (ScanCursor. allocator (RootCache. allocator buffer-pool)
                                            col-names col-preds
                                            temporal-bounds
                                            (.iterator ^Iterable merge-tasks)
                                            schema args))))))}))

      (close [_]
        (->> table->template-rel+trie vals (map first) util/close)))))

(defmethod ig/halt-key! ::scan-emitter [_ ^IScanEmitter scan-emmiter]
  (.close scan-emmiter))

(defmethod lp/emit-expr :scan [scan-expr {:keys [^IScanEmitter scan-emitter db scan-fields, param-fields]}]
  (.emitScan scan-emitter db scan-expr scan-fields param-fields))
