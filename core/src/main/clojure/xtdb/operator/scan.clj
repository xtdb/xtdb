(ns xtdb.operator.scan
  (:require [clojure.spec.alpha :as s]
            [integrant.core :as ig]
            [xtdb.expression :as expr]
            [xtdb.expression.metadata :as expr.meta]
            xtdb.indexer.live-index
            [xtdb.information-schema :as info-schema]
            [xtdb.logical-plan :as lp]
            [xtdb.metadata :as meta]
            xtdb.object-store
            [xtdb.table-catalog :as table-cat]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as cat]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (clojure.lang MapEntry)
           java.nio.ByteBuffer
           java.time.Instant
           (java.util LinkedList)
           (java.util.function IntPredicate Predicate)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           [org.apache.arrow.memory.util ArrowBufPointer]
           [org.roaringbitmap.buffer MutableRoaringBitmap]
           (xtdb BufferPool)
           (xtdb.bloom BloomUtils)
           (xtdb.indexer LiveTable$Watermark Watermark Watermark$Source)
           (xtdb.metadata PageMetadata PageMetadata$Factory)
           (xtdb.operator.scan IidSelector MergePlanPage$Arrow MergePlanPage$Memory RootCache ScanCursor ScanCursor$MergeTask)
           (xtdb.trie ArrowHashTrie$Leaf HashTrie HashTrieKt MergePlanNode MergePlanTask Trie TrieCatalog)
           (xtdb.util TemporalBounds TemporalDimension)
           (xtdb.vector RelationReader)))

(s/def ::table symbol?)

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
  (scanFields [^xtdb.indexer.Watermark wm, scan-cols])
  (emitScan [scan-expr scan-fields param-fields]))

(defn ->scan-cols [{:keys [columns], {:keys [table]} :scan-opts}]
  (for [[col-tag col-arg] columns]
    [table (case col-tag
             :column col-arg
             :select (key (first col-arg)))]))

(def ^:dynamic *column->pushdown-bloom* {})

(defn- ->temporal-bounds [^RelationReader args, {:keys [for-valid-time for-system-time]}, ^Instant snapshot-time]
  (letfn [(->time-μs [[tag arg]]
            (case tag
              :literal (-> arg
                           (time/sql-temporal->micros expr/*default-tz*))
              :param (some-> (-> (.vectorForOrNull args (name arg))
                                 (.getObject 0))
                             (time/sql-temporal->micros expr/*default-tz*))
              :now (-> (expr/current-time)
                       (time/instant->micros))))
          (apply-constraint [constraint]
            (if-let [[tag & args] constraint]
              (case tag
                :at (let [[at] args
                          at-μs (->time-μs at)]
                      (TemporalDimension/at at-μs))

                ;; overlaps [time-from time-to]
                :in (let [[from to] args]
                      (TemporalDimension/in (->time-μs (or from [:now]))
                                            (some-> to ->time-μs)))

                :between (let [[from to] args]
                           (TemporalDimension/between (->time-μs (or from [:now]))
                                                      (some-> to ->time-μs)))

                :all-time (TemporalDimension.))
              (TemporalDimension.)))]

    (let [^TemporalDimension sys-dim (apply-constraint for-system-time)
          bounds (TemporalBounds. (apply-constraint for-valid-time) sys-dim)]
      ;; we further constrain bases on tx
      (when-let [system-time (some-> snapshot-time time/instant->micros)]
        (.setUpper sys-dim (min (inc system-time) (.getUpper sys-dim)))

        (when-not for-system-time
          (.setLower (.getSystemTime bounds) system-time)))

      bounds)))

(defn tables-with-cols [^Watermark$Source wm-src]
  (with-open [wm (.openWatermark wm-src)]
    (.getSchema wm)))

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
                        (.vectorForOrNull "bloom"))]
      (reify IntPredicate
        (test [_ page-idx]
          (boolean
            (let [bloom-vec-idx (.rowIndex page-metadata col-name page-idx)]
              (and (>= bloom-vec-idx 0)
                   (not (nil? (.getObject bloom-rdr bloom-vec-idx)))
                   (MutableRoaringBitmap/intersects pushdown-bloom (BloomUtils/bloomToBitmap bloom-rdr bloom-vec-idx))))))))))

(defn ->path-pred [^ArrowBuf iid-arrow-buf]
  (when iid-arrow-buf
    (let [iid-ptr (ArrowBufPointer. iid-arrow-buf 0 (.capacity iid-arrow-buf))]
      (reify Predicate
        (test [_ path]
          (zero? (HashTrie/compareToPath iid-ptr path)))))))

(defmethod ig/prep-key ::scan-emitter [_ opts]
  (merge opts
         {:allocator (ig/ref :xtdb/allocator)
          :metadata-mgr (ig/ref ::meta/metadata-manager)
          :buffer-pool (ig/ref :xtdb/buffer-pool)
          :table-catalog (ig/ref :xtdb/table-catalog)
          :trie-catalog (ig/ref :xtdb/trie-catalog)}))

(defmethod ig/init-key ::scan-emitter [_ {:keys [^BufferAllocator allocator, ^PageMetadata$Factory metadata-mgr, ^BufferPool buffer-pool,
                                                 ^TrieCatalog trie-catalog, table-catalog]}]
  (let [table->template-rel+trie (info-schema/table->template-rel+tries allocator)]
    (reify IScanEmitter
      (close [_] (->> table->template-rel+trie vals (map first) util/close))
      (scanFields [_ wm scan-cols]
        (letfn [(->field [[table col-name]]
                  (let [table (str table)
                        col-name (str col-name)]
                    ;; TODO move to fields here
                    (-> (or (some-> (types/temporal-col-types col-name) types/col-type->field)
                            (get-in info-schema/derived-tables [(symbol table) (symbol col-name)])
                            (get-in info-schema/template-tables [(symbol table) (symbol col-name)])
                            (types/merge-fields (table-cat/column-field table-catalog table col-name)
                                                (some-> (.getLiveIndex wm)
                                                        (.liveTable table)
                                                        (.columnField col-name))))
                        (types/field-with-name col-name))))]
          (->> scan-cols
               (into {} (map (juxt identity ->field))))))

      (emitScan [_ {:keys [columns], {:keys [table] :as scan-opts} :scan-opts} scan-fields param-fields]
        (let [col-names (->> columns
                             (into #{} (map (fn [[col-type arg]]
                                              (case col-type
                                                :column arg
                                                :select (key (first arg)))))))
              fields (->> col-names
                          (into {} (map (juxt identity
                                              (fn [col-name]
                                                (get scan-fields [table col-name]))))))

              col-names (into #{} (map str) col-names)

              table-name (str table)

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

              row-count (table-cat/row-count table-catalog table-name)]

          {:fields fields
           :stats {:row-count row-count}
           :->cursor (fn [{:keys [allocator, ^Watermark watermark, snapshot-time, schema, args]}]
                       (if (and (info-schema/derived-tables table) (not (info-schema/template-tables table)))
                         (let [derived-table-schema (info-schema/derived-tables table)]
                           (info-schema/->cursor allocator derived-table-schema table col-names col-preds schema args table-catalog trie-catalog watermark))

                         (let [template-table? (info-schema/template-tables table)
                               iid-bb (selects->iid-byte-buffer selects args)
                               col-preds (cond-> col-preds
                                           iid-bb (assoc "_iid" (IidSelector. iid-bb)))
                               metadata-pred (expr.meta/->metadata-selector allocator (cons 'and metadata-args) (update-vals fields types/field->col-type) args)
                               scan-opts (-> scan-opts
                                             (update :for-valid-time
                                                     (fn [fvt]
                                                       (or fvt [:at [:now :now]]))))
                               ^LiveTable$Watermark live-table-wm (some-> (.getLiveIndex watermark) (.liveTable table-name))
                               temporal-bounds (->temporal-bounds args scan-opts snapshot-time)]
                           (util/with-open [iid-arrow-buf (when iid-bb (util/->arrow-buf-view allocator iid-bb))]
                             (let [merge-tasks (util/with-open [page-metadatas (LinkedList.)]
                                                 (let [segments (cond-> (mapv (fn [{:keys [trie-key]}]
                                                                                (let [meta-path (Trie/metaFilePath table-name trie-key)
                                                                                      page-metadata (.openPageMetadata metadata-mgr meta-path)]
                                                                                  (.add page-metadatas page-metadata)
                                                                                  (into (trie/->Segment (.getTrie page-metadata))
                                                                                        {:data-file-path (Trie/dataFilePath table-name trie-key)
                                                                                         :page-idx-pred (reduce (fn [^IntPredicate page-idx-pred col-name]
                                                                                                                  (if-let [bloom-page-idx-pred (filter-pushdown-bloom-page-idx-pred page-metadata col-name)]
                                                                                                                    (.and page-idx-pred bloom-page-idx-pred)
                                                                                                                    page-idx-pred))
                                                                                                                (.build metadata-pred page-metadata)
                                                                                                                col-names)
                                                                                         :page-metadata page-metadata})))
                                                                              (-> (cat/trie-state trie-catalog table-name)
                                                                                  (cat/current-tries)
                                                                                  (cat/filter-tries temporal-bounds)))

                                                                  live-table-wm (conj (-> (trie/->Segment (.getLiveTrie live-table-wm))
                                                                                          (assoc :memory-rel (.getLiveRelation live-table-wm))))
                                                                  template-table? (conj (let [[memory-rel trie] (table->template-rel+trie (symbol table-name))]
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
                                            schema args))))))})))))

(defmethod ig/halt-key! ::scan-emitter [_ ^IScanEmitter scan-emmiter]
  (.close scan-emmiter))

(defmethod lp/emit-expr :scan [scan-expr {:keys [^IScanEmitter scan-emitter scan-fields, param-fields]}]
  (.emitScan scan-emitter scan-expr scan-fields param-fields))
