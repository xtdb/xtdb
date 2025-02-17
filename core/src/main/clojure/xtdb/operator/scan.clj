(ns xtdb.operator.scan
  (:require [clojure.spec.alpha :as s]
            [integrant.core :as ig]
            [xtdb.bitemporal :as bitemp]
            [xtdb.bloom :as bloom]
            [xtdb.buffer-pool :as bp]
            [xtdb.expression :as expr]
            [xtdb.expression.metadata :as expr.meta]
            xtdb.indexer.live-index
            [xtdb.information-schema :as info-schema]
            [xtdb.logical-plan :as lp]
            [xtdb.metadata :as meta]
            xtdb.object-store
            [xtdb.time :as time]
            [xtdb.trie :as trie :refer [MergePlanPage]]
            [xtdb.trie-catalog :as cat]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang MapEntry)
           (com.carrotsearch.hppc IntArrayList)
           (java.io Closeable)
           java.nio.ByteBuffer
           (java.nio.file Path)
           java.time.Instant
           (java.util ArrayList Comparator HashMap Iterator LinkedList Map PriorityQueue Stack)
           (java.util.function BiFunction IntPredicate Predicate)
           (java.util.stream IntStream)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           [org.apache.arrow.memory.util ArrowBufPointer]
           (org.apache.arrow.vector VectorLoader)
           (org.apache.arrow.vector.types.pojo Field FieldType)
           [org.roaringbitmap.buffer MutableRoaringBitmap]
           (xtdb BufferPool ICursor)
           (xtdb.arrow VectorIndirection VectorReader)
           (xtdb.bitemporal IRowConsumer Polygon)
           (xtdb.indexer LiveTable$Watermark Watermark Watermark$Source)
           (xtdb.metadata IMetadataManager ITableMetadata)
           xtdb.operator.SelectionSpec
           (xtdb.trie ArrowHashTrie$Leaf EventRowPointer EventRowPointer$Arrow HashTrie HashTrieKt MemoryHashTrie$Leaf MergePlanNode MergePlanTask TrieCatalog)
           (xtdb.util TemporalBounds TemporalDimension)
           (xtdb.vector IMultiVectorRelationFactory IRelationWriter IVectorReader IVectorWriter IndirectMultiVectorReader RelationReader RelationWriter)))

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
              :param (some-> (-> (.readerForName args (name arg))
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

(defn temporal-column? [col-name]
  (contains? #{"_system_from" "_system_to" "_valid_from" "_valid_to"}
             col-name))

(defn rels->multi-vector-rel-factory ^xtdb.vector.IMultiVectorRelationFactory [leaf-rels, ^BufferAllocator allocator, col-names]
  (let [put-rdrs (mapv (fn [^RelationReader rel]
                         [(.rowCount rel) (-> (.readerForName rel "op") (.legReader "put"))])
                       leaf-rels)
        reader-indirection (IntArrayList.)
        vector-indirection (IntArrayList.)]
    (letfn [(->indirect-multi-vec [col-name reader-selection vector-selection]
              (let [readers (ArrayList.)]
                (if (= col-name "_iid")
                  (doseq [^RelationReader leaf-rel leaf-rels]
                    (.add readers (.readerForName leaf-rel "_iid")))

                  (doseq [[row-count ^IVectorReader put-rdr] put-rdrs]
                    (if-let [rdr (some-> (.structKeyReader put-rdr col-name)
                                         (.withName col-name))]
                      (.add readers rdr)
                      (.add readers (vr/->absent-col col-name allocator row-count)))))
                (IndirectMultiVectorReader. readers reader-selection vector-selection)))]
      (reify IMultiVectorRelationFactory
        (accept [_ rdrIdx vecIdx]
          (.add reader-indirection rdrIdx)
          (.add vector-indirection vecIdx))
        (realize [_]
          (let [reader-selection (VectorIndirection/selection (.toArray reader-indirection))
                vector-selection (VectorIndirection/selection (.toArray vector-indirection))]
            (RelationReader/from (mapv #(->indirect-multi-vec % reader-selection vector-selection) col-names))))))))

(defn- ->content-rel-factory ^xtdb.vector.IMultiVectorRelationFactory [leaf-rdrs allocator content-col-names]
  (rels->multi-vector-rel-factory leaf-rdrs allocator content-col-names))

(defn- ->bitemporal-consumer ^xtdb.bitemporal.IRowConsumer [^IRelationWriter out-rel, col-names]
  (letfn [(writer-for [col-name nullable?]
            (when (contains? col-names col-name)
              (.colWriter out-rel col-name (FieldType. nullable? (types/->arrow-type types/temporal-col-type) nil))))]

    (let [^IVectorWriter valid-from-wtr (writer-for "_valid_from" false)
          ^IVectorWriter valid-to-wtr (writer-for "_valid_to" true)
          ^IVectorWriter sys-from-wtr (writer-for "_system_from" false)
          ^IVectorWriter sys-to-wtr (writer-for "_system_to" true)]

      (reify IRowConsumer
        (accept [_ valid-from valid-to sys-from sys-to]
          (some-> valid-from-wtr (.writeLong valid-from))

          (when valid-to-wtr
            (if (= Long/MAX_VALUE valid-to)
              (.writeNull valid-to-wtr)
              (.writeLong valid-to-wtr valid-to)))

          (some-> sys-from-wtr (.writeLong sys-from))

          (when sys-to-wtr
            (if (= Long/MAX_VALUE sys-to)
              (.writeNull sys-to-wtr)
              (.writeLong sys-to-wtr sys-to))))))))

(defn iid-selector [^ByteBuffer iid-bb]
  (reify SelectionSpec
    (select [_ allocator rel-rdr _schema _args]
      (with-open [arrow-buf (util/->arrow-buf-view allocator iid-bb)]
        (let [iid-ptr (ArrowBufPointer. arrow-buf 0 (.capacity iid-bb))
              ptr (ArrowBufPointer.)
              iid-rdr (.readerForName rel-rdr "_iid")
              value-count (.valueCount iid-rdr)]
          (if (pos-int? value-count)
            ;; lower-bound
            (loop [left 0 right (dec value-count)]
              (if (= left right)
                (if (= iid-ptr (.getPointer iid-rdr left ptr))
                  ;; upper bound
                  (loop [right left]
                    (if (or (>= right value-count) (not= iid-ptr (.getPointer iid-rdr right ptr)))
                      (.toArray (IntStream/range left right))
                      (recur (inc right))))
                  (int-array 0))
                (let [mid (quot (+ left right) 2)]
                  (if (<= (.compareTo iid-ptr (.getPointer iid-rdr mid ptr)) 0)
                    (recur left mid)
                    (recur (inc mid) right)))))
            (int-array 0)))))))


(defrecord VSRCache [^BufferPool buffer-pool, ^BufferAllocator allocator, ^Map free, ^Map used]
  Closeable
  (close [_]
    (util/close free)
    (util/close used)))

(defn ->vsr-cache [buffer-pool allocator]
  (->VSRCache buffer-pool allocator (HashMap.) (HashMap.)))

(defn reset-vsr-cache [{:keys [^Map free, ^Map used]}]
  (doseq [^MapEntry entry (.entrySet used)]
    (.merge free (key entry) (val entry) (reify BiFunction
                                           (apply [_ free-entries used-entries]
                                             (.addAll ^Stack free-entries ^Stack used-entries)
                                             free-entries))))
  (.clear used))

(defn cache-vsr [{:keys [^Map free, ^Map used, buffer-pool, allocator]} ^Path trie-leaf-file]
  (let [vsr (let [^Stack free-entries (.get free trie-leaf-file)]
              (if (and free-entries (> (.size free-entries) 0))
                (.pop free-entries)
                (bp/open-vsr buffer-pool trie-leaf-file allocator)))
        ^Stack used-entries (.computeIfAbsent used trie-leaf-file
                                              (fn [_]
                                                (Stack.)))]
    (.push used-entries vsr)
    vsr))

(defrecord LeafPointer [ev-ptr rel-idx])

(deftype TrieCursor [^BufferAllocator allocator, ^Iterator merge-tasks, ^IRelationWriter out-rel
                     col-names, ^Map col-preds,
                     ^TemporalBounds temporal-bounds
                     schema, args, vsr-cache, buffer-pool]
  ICursor
  (tryAdvance [_ c]
    (let [!advanced? (boolean-array 1)]
      (while (and (not (aget !advanced? 0))
                  (.hasNext merge-tasks))
        (let [{:keys [leaves path]} (.next merge-tasks)
              is-valid-ptr (ArrowBufPointer.)]
          (reset-vsr-cache vsr-cache)
          (with-open [out-rel (vw/->rel-writer allocator)]
            (let [^SelectionSpec iid-pred (get col-preds "_iid")
                  merge-q (PriorityQueue. (Comparator/comparing #(.ev_ptr ^LeafPointer %) (EventRowPointer/comparator)))
                  calculate-polygon (bitemp/polygon-calculator temporal-bounds)
                  bitemp-consumer (->bitemporal-consumer out-rel col-names)
                  leaf-rdrs (for [leaf leaves
                                  :let [^RelationReader data-rdr (trie/load-page leaf buffer-pool vsr-cache)]]
                              (cond-> data-rdr
                                iid-pred (.select (.select iid-pred allocator data-rdr {} args))))
                  [temporal-cols content-cols] ((juxt filter remove) temporal-column? col-names)
                  content-rel-factory (->content-rel-factory leaf-rdrs allocator content-cols)]

              (doseq [[idx leaf-rdr] (map-indexed vector leaf-rdrs)
                      :let [ev-ptr (EventRowPointer$Arrow. leaf-rdr path)]]
                (when (.isValid ev-ptr is-valid-ptr path)
                  (.add merge-q (->LeafPointer ev-ptr idx))))

              (loop []
                (when-let [^LeafPointer q-obj (.poll merge-q)]
                  (let [^EventRowPointer ev-ptr (.ev_ptr q-obj)]
                    (when-let [^Polygon polygon (calculate-polygon ev-ptr)]
                      (when (= "put" (.getOp ev-ptr))
                        (let [sys-from (.getSystemFrom ev-ptr)
                              idx (.getIndex ev-ptr)]
                          (dotimes [i (.getValidTimeRangeCount polygon)]
                            (let [valid-from (.getValidFrom polygon i)
                                  valid-to (.getValidTo polygon i)
                                  sys-to (.getSystemTo polygon i)]
                              (when (and (.intersects temporal-bounds valid-from valid-to sys-from sys-to)
                                         (not (= valid-from valid-to))
                                         (not (= sys-from sys-to)))
                                (.startRow out-rel)
                                (.accept content-rel-factory (.rel-idx q-obj) idx)
                                (.accept bitemp-consumer valid-from valid-to sys-from sys-to)
                                (.endRow out-rel)))))))

                    (.nextIndex ev-ptr)
                    (when (.isValid ev-ptr is-valid-ptr path)
                      (.add merge-q q-obj))
                    (recur))))

              (let [^RelationReader rel (cond-> (.realize content-rel-factory)
                                          (or (empty? (seq content-cols)) (seq temporal-cols))
                                          (vr/concat-rels (vw/rel-wtr->rdr out-rel)))
                    ^RelationReader rel (reduce (fn [^RelationReader rel ^SelectionSpec col-pred]
                                                  (.select rel (.select col-pred allocator rel schema args)))
                                                rel
                                                (vals (dissoc col-preds "_iid")))]
                (when (pos? (.rowCount rel))
                  (.accept c rel)
                  (aset !advanced? 0 true)))))))

      (aget !advanced? 0)))

  (close [_]
    (util/close vsr-cache)
    (util/close out-rel)))

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
          (let [eid-rdr (.readerForName args-rel (name eid))]
            (when (= 1 (.valueCount eid-rdr))
              (let [eid (.getObject eid-rdr 0)]
                (if (util/valid-iid? eid)
                  (util/->iid eid)
                  dummy-iid)))))))))

(defn filter-pushdown-bloom-page-idx-pred ^IntPredicate [^ITableMetadata table-metadata ^String col-name]
  (when-let [^MutableRoaringBitmap pushdown-bloom (get *column->pushdown-bloom* (symbol col-name))]
    (let [metadata-rdr (VectorReader/from (.metadataReader table-metadata))
          bloom-rdr (-> (.keyReader metadata-rdr "columns")
                        (.elementReader)
                        (.keyReader "bloom"))]
      (reify IntPredicate
        (test [_ page-idx]
          (boolean
           (let [bloom-vec-idx (.rowIndex table-metadata col-name page-idx)]
             (and (>= bloom-vec-idx 0)
                  (not (nil? (.getObject bloom-rdr bloom-vec-idx)))
                  (MutableRoaringBitmap/intersects pushdown-bloom
                                                   (bloom/bloom->bitmap bloom-rdr bloom-vec-idx))))))))))

(defn ->path-pred [^ArrowBuf iid-arrow-buf]
  (when iid-arrow-buf
    (let [iid-ptr (ArrowBufPointer. iid-arrow-buf 0 (.capacity iid-arrow-buf))]
      (reify Predicate
        (test [_ path]
          (zero? (HashTrie/compareToPath iid-ptr path)))))))

(defrecord ArrowMergePlanPage [data-file-path ^IntPredicate page-idx-pred ^long page-idx ^ITableMetadata table-metadata]
  MergePlanPage
  (load-page [_mpg buffer-pool vsr-cache]
    (let [^BufferPool bp buffer-pool]
      (util/with-open [rb (.getRecordBatch bp data-file-path page-idx)]
        (let [vsr (cache-vsr vsr-cache data-file-path)
              loader (VectorLoader. vsr)]
          (.load loader rb)
          (vr/<-root vsr)))))

  (test-metadata [_mpg]
    (.test page-idx-pred page-idx))

  (temporal-bounds [_mpg] (.temporalBounds table-metadata (int page-idx))))

(def ^:private non-constraint-bounds (TemporalBounds.))

(defrecord MemoryMergePlanPage [^RelationReader live-rel trie ^MemoryHashTrie$Leaf leaf]
  MergePlanPage
  (load-page [_mpg _buffer-pool _vsr-cache]
    (.select live-rel (.mergeSort leaf trie)))

  (test-metadata [_mpg] true)

  (temporal-bounds [_msg] non-constraint-bounds))

(defmethod ig/prep-key ::scan-emitter [_ opts]
  (merge opts
         {:allocator (ig/ref :xtdb/allocator)
          :metadata-mgr (ig/ref ::meta/metadata-manager)
          :buffer-pool (ig/ref :xtdb/buffer-pool)
          :trie-catalog (ig/ref :xtdb/trie-catalog)}))

(defmethod ig/init-key ::scan-emitter [_ {:keys [^BufferAllocator allocator, ^IMetadataManager metadata-mgr, ^BufferPool buffer-pool,
                                                 ^TrieCatalog trie-catalog]}]
  (let [table->template-rel+trie (info-schema/table->template-rel+tries allocator)]
    (reify IScanEmitter
      (close [_] (->> table->template-rel+trie vals (map first) util/close))
      (scanFields [_ wm scan-cols]
        (letfn [(->field [[table col-name]]
                  (let [table (str table)
                        col-name (str col-name)]
                    ;; TODO move to fields here
                    (-> (or (some-> (types/temporal-col-types col-name) types/col-type->field)
                            (or (get-in info-schema/derived-tables [(symbol table) (symbol col-name)])
                                (get-in info-schema/template-tables [(symbol table) (symbol col-name)])
                                (types/merge-fields (.columnField metadata-mgr table col-name)
                                                    (some-> (.getLiveIndex wm)
                                                            (.liveTable table)
                                                            (.columnField col-name)))))
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

              row-count (-> (.latestBlockMetadata metadata-mgr)
                            (get-in [:tables table-name :row-count]))]

          {:fields fields
           :stats {:row-count row-count}
           :->cursor (fn [{:keys [allocator, ^Watermark watermark, snapshot-time, schema, args]}]
                       (if (and (info-schema/derived-tables table) (not (info-schema/template-tables table)))
                         (let [derived-table-schema (info-schema/derived-tables table)]
                           (info-schema/->cursor allocator derived-table-schema table col-names col-preds schema args metadata-mgr watermark))

                         (let [template-table? (info-schema/template-tables table)
                               iid-bb (selects->iid-byte-buffer selects args)
                               col-preds (cond-> col-preds
                                           iid-bb (assoc "_iid" (iid-selector iid-bb)))
                               metadata-pred (expr.meta/->metadata-selector (cons 'and metadata-args) (update-vals fields types/field->col-type) args)
                               scan-opts (-> scan-opts
                                             (update :for-valid-time
                                                     (fn [fvt]
                                                       (or fvt [:at [:now :now]]))))
                               ^LiveTable$Watermark live-table-wm (some-> (.getLiveIndex watermark) (.liveTable table-name))
                               temporal-bounds (->temporal-bounds args scan-opts snapshot-time)]
                           (util/with-open [iid-arrow-buf (when iid-bb (util/->arrow-buf-view allocator iid-bb))]
                             (let [merge-tasks (util/with-open [table-metadatas (LinkedList.)]
                                                 (let [segments (cond-> (mapv (fn [{:keys [trie-key]}]
                                                                                (let [meta-path (trie/->table-meta-file-path table-name trie-key)
                                                                                      {:keys [trie] :as table-metadata} (.openTableMetadata metadata-mgr meta-path)]
                                                                                  (.add table-metadatas table-metadata)
                                                                                  (into (trie/->Segment trie)
                                                                                        {:data-file-path (trie/->table-data-file-path table-name trie-key)
                                                                                         :page-idx-pred (reduce (fn [^IntPredicate page-idx-pred col-name]
                                                                                                                  (if-let [bloom-page-idx-pred (filter-pushdown-bloom-page-idx-pred table-metadata col-name)]
                                                                                                                    (.and page-idx-pred bloom-page-idx-pred)
                                                                                                                    page-idx-pred))
                                                                                                                (.build metadata-pred table-metadata)
                                                                                                                col-names)
                                                                                         :table-metadata table-metadata})))
                                                                              (-> (cat/trie-state trie-catalog table-name)
                                                                                  (cat/current-tries)))

                                                                  live-table-wm (conj (-> (trie/->Segment (.liveTrie live-table-wm))
                                                                                          (assoc :memory-rel (.liveRelation live-table-wm))))
                                                                  template-table? (conj (let [[memory-rel trie] (table->template-rel+trie (symbol table-name))]
                                                                                          (-> (trie/->Segment trie)
                                                                                              (assoc :memory-rel memory-rel)))))]
                                                   (->> (HashTrieKt/toMergePlan segments (->path-pred iid-arrow-buf) temporal-bounds)
                                                        (into [] (keep (fn [^MergePlanTask mpt]
                                                                         (when-let [leaves (trie/->merge-task
                                                                                            (for [^MergePlanNode mpn (.getMpNodes mpt)
                                                                                                  :let [{:keys [data-file-path table-metadata page-idx-pred trie memory-rel]} (.getSegment mpn)
                                                                                                        node (.getNode mpn)]]
                                                                                              (if data-file-path
                                                                                                (->ArrowMergePlanPage data-file-path
                                                                                                                      page-idx-pred
                                                                                                                      (.getDataPageIndex ^ArrowHashTrie$Leaf node)
                                                                                                                      table-metadata)
                                                                                                (->MemoryMergePlanPage memory-rel trie node)))
                                                                                            temporal-bounds)]
                                                                           {:path (.getPath mpt)
                                                                            :leaves leaves})))))))]

                               (util/with-close-on-catch [out-rel (RelationWriter. allocator
                                                                                   (for [^Field field (vals fields)]
                                                                                     (vw/->writer (.createVector field allocator))))]
                                 (->TrieCursor allocator (.iterator ^Iterable merge-tasks) out-rel
                                               col-names col-preds
                                               temporal-bounds
                                               schema
                                               args
                                               (->vsr-cache buffer-pool allocator)
                                               buffer-pool)))))))})))))

(defmethod ig/halt-key! ::scan-emitter [_ ^IScanEmitter scan-emmiter]
  (.close scan-emmiter))

(defmethod lp/emit-expr :scan [scan-expr {:keys [^IScanEmitter scan-emitter scan-fields, param-fields]}]
  (.emitScan scan-emitter scan-expr scan-fields param-fields))
