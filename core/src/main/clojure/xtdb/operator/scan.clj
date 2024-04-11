(ns xtdb.operator.scan
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [juxt.clojars-mirrors.integrant.core :as ig]
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
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]
            [xtdb.xtql.edn :as edn])
  (:import (clojure.lang MapEntry)
           (com.carrotsearch.hppc IntArrayList)
           (java.io Closeable)
           java.nio.ByteBuffer
           (java.nio.file Path)
           (java.util ArrayList Comparator HashMap Iterator LinkedList Map PriorityQueue Stack)
           (java.util.function IntPredicate Predicate BiFunction)
           (java.util.stream IntStream)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           [org.apache.arrow.memory.util ArrowBufPointer]
           (org.apache.arrow.vector VectorLoader)
           (org.apache.arrow.vector.types.pojo Field FieldType)
           [org.roaringbitmap.buffer MutableRoaringBitmap]
           xtdb.api.TransactionKey
           (xtdb.bitemporal IRowConsumer Polygon)
           xtdb.IBufferPool
           xtdb.ICursor
           (xtdb.metadata IMetadataManager ITableMetadata)
           xtdb.operator.IRelationSelector
           (xtdb.trie ArrowHashTrie$Leaf EventRowPointer HashTrie HashTrieKt LiveHashTrie$Leaf MergePlanNode MergePlanTask)
           (xtdb.util TemporalBounds TemporalBounds$TemporalColumn)
           (xtdb.vector IMultiVectorRelationFactory IRelationWriter IVectorIndirection$Selection IVectorReader
                        IVectorWriter IndirectMultiVectorReader RelationReader RelationWriter)
           (xtdb.watermark ILiveTableWatermark IWatermarkSource Watermark)))

(s/def ::table symbol?)

;; TODO be good to just specify a single expression here and have the interpreter split it
;; into metadata + col-preds - the former can accept more than just `(and ~@col-preds)
(defmethod lp/ra-expr :scan [_]
  (s/cat :op #{:scan}
         :scan-opts (s/keys :req-un [::table]
                            :opt-un [::lp/for-valid-time ::lp/for-system-time ::lp/default-all-valid-time?])
         :columns (s/coll-of (s/or :column ::lp/column
                                   :select ::lp/column-expression))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IScanEmitter
  (tableColNames [^xtdb.watermark.Watermark wm, ^String table-name])
  (allTableColNames [^xtdb.watermark.Watermark wm])
  (scanFields [^xtdb.watermark.Watermark wm, scan-cols])
  (emitScan [scan-expr scan-fields param-fields]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn ->scan-cols [{:keys [columns], {:keys [table]} :scan-opts}]
  (for [[col-tag col-arg] columns]
    [table (case col-tag
             :column col-arg
             :select (key (first col-arg)))]))

(def ^:dynamic *column->pushdown-bloom* {})

(defn- ->temporal-bounds [^RelationReader params, {:keys [^TransactionKey at-tx]}, {:keys [for-valid-time for-system-time]}]
  (let [bounds (TemporalBounds.)]
    (letfn [(->time-μs [[tag arg]]
              (case tag
                :literal (-> arg
                             (time/sql-temporal->micros (.getZone expr/*clock*)))
                :param (some-> (-> (.readerForName params (name arg))
                                   (.getObject 0))
                               (time/sql-temporal->micros (.getZone expr/*clock*)))
                :now (-> (.instant expr/*clock*)
                         (time/instant->micros))))]

      (when-let [system-time (some-> at-tx (.getSystemTime) time/instant->micros)]
        (.lte (.getSystemFrom bounds) system-time)

        (when-not for-system-time
          (.gt (.getSystemTo bounds) system-time)))

      (letfn [(apply-constraint [constraint ^TemporalBounds$TemporalColumn start-col, ^TemporalBounds$TemporalColumn end-col]
                (when-let [[tag & args] constraint]
                  (case tag
                    :at (let [[at] args
                              at-μs (->time-μs at)]
                          (.lte start-col at-μs)
                          (.gt end-col at-μs))

                    ;; overlaps [time-from time-to]
                    :in (let [[from to] args]
                          (.gt end-col (->time-μs (or from [:now])))
                          (when-let [to-μs (some-> to ->time-μs)]
                            (.lt start-col to-μs)))

                    :between (let [[from to] args]
                               (.gt end-col (->time-μs (or from [:now])))
                               (when-let [to-μs (some-> to ->time-μs)]
                                 (.lte start-col to-μs)))

                    :all-time nil)))]

        (apply-constraint for-valid-time (.getValidFrom bounds) (.getValidTo bounds))
        (apply-constraint for-system-time (.getSystemFrom bounds) (.getSystemTo bounds))))
    bounds))

(defn tables-with-cols [{:keys [basis after-tx]} ^IWatermarkSource wm-src ^IScanEmitter scan-emitter]
  (let [{:keys [at-tx]} basis
        wm-tx (or at-tx after-tx)]
    (with-open [^Watermark wm (.openWatermark wm-src wm-tx)]
      (.allTableColNames scan-emitter wm))))

(defn temporal-column? [col-name]
  (contains? #{"xt$system_from" "xt$system_to" "xt$valid_from" "xt$valid_to"}
             (util/str->normal-form-str col-name)))

(defn rels->multi-vector-rel-factory ^xtdb.vector.IMultiVectorRelationFactory [leaf-rels, ^BufferAllocator allocator, col-names]
  (let [put-rdrs (mapv (fn [^RelationReader rel]
                         [(.rowCount rel) (-> (.readerForName rel "op") (.legReader :put))])
                       leaf-rels)
        reader-indirection (IntArrayList.)
        vector-indirection (IntArrayList.)]
    (letfn [(->indirect-multi-vec [col-name reader-selection vector-selection]
              (let [normalized-name (util/str->normal-form-str col-name)
                    readers (ArrayList.)]
                (if (= normalized-name "xt$iid")
                  (doseq [^RelationReader leaf-rel leaf-rels]
                    (.add readers (-> (.readerForName leaf-rel "xt$iid") (.withName col-name))))

                  (doseq [[row-count ^IVectorReader put-rdr] put-rdrs]
                    (if-let [rdr (some-> (.structKeyReader put-rdr normalized-name)
                                         (.withName col-name))]
                      (.add readers rdr)
                      (.add readers (vr/->absent-col col-name allocator row-count)))))
                (IndirectMultiVectorReader. readers reader-selection vector-selection)))]
      (reify IMultiVectorRelationFactory
        (accept [_ rdrIdx vecIdx]
          (.add reader-indirection rdrIdx)
          (.add vector-indirection vecIdx))
        (realize [_]
          (let [reader-selection (IVectorIndirection$Selection. (.toArray reader-indirection))
                vector-selection (IVectorIndirection$Selection. (.toArray vector-indirection))]
            (RelationReader/from (mapv #(->indirect-multi-vec % reader-selection vector-selection) col-names))))))))

(defn- ->content-rel-factory ^xtdb.vector.IMultiVectorRelationFactory [leaf-rdrs allocator content-col-names]
  (rels->multi-vector-rel-factory leaf-rdrs allocator content-col-names))

(defn- ->bitemporal-consumer ^xtdb.bitemporal.IRowConsumer [^IRelationWriter out-rel, col-names]
  (letfn [(writer-for [normalised-col-name]
            (when-let [wtrs (not-empty
                             (->> col-names
                                  (into [] (keep (fn [^String col-name]
                                                   (when (= normalised-col-name (util/str->normal-form-str col-name))
                                                     (.colWriter out-rel col-name (FieldType/notNullable (types/->arrow-type types/temporal-col-type)))))))))]
              (reify IVectorWriter
                (writeLong [_ l]
                  (doseq [^IVectorWriter wtr wtrs]
                    (.writeLong wtr l))))))]

    (let [^IVectorWriter valid-from-wtr (writer-for "xt$valid_from")
          ^IVectorWriter valid-to-wtr (writer-for "xt$valid_to")
          ^IVectorWriter sys-from-wtr (writer-for "xt$system_from")
          ^IVectorWriter sys-to-wtr (writer-for "xt$system_to")]

      (reify IRowConsumer
        (accept [_ _idx valid-from valid-to sys-from sys-to]
          (some-> valid-from-wtr (.writeLong valid-from))
          (some-> valid-to-wtr (.writeLong valid-to))
          (some-> sys-from-wtr (.writeLong sys-from))
          (some-> sys-to-wtr (.writeLong sys-to)))))))

(defn iid-selector [^ByteBuffer iid-bb]
  (reify IRelationSelector
    (select [_ allocator rel-rdr _params]
      (with-open [arrow-buf (util/->arrow-buf-view allocator iid-bb)]
        (let [iid-ptr (ArrowBufPointer. arrow-buf 0 (.capacity iid-bb))
              ptr (ArrowBufPointer.)
              iid-rdr (.readerForName rel-rdr "xt$iid")
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


(defrecord VSRCache [^IBufferPool buffer-pool, ^BufferAllocator allocator, ^Map free, ^Map used]
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
                                              (util/->jfn
                                                (fn [_]
                                                  (Stack.))))]
    (.push used-entries vsr)
    vsr))

(defn merge-task-data-reader ^IVectorReader [buffer-pool vsr-cache [leaf-tag & leaf-args]]
  (case leaf-tag
    :arrow
    (let [[{:keys [data-file-path]} page-idx] leaf-args]
      (util/with-open [rb (bp/open-record-batch buffer-pool data-file-path page-idx)]
        (let [vsr (cache-vsr vsr-cache data-file-path)
              loader (VectorLoader. vsr)]
          (.load loader rb)
          (vr/<-root vsr))))

    :live (first leaf-args)))

(defrecord LeafPointer [ev-ptr rel-idx])

(deftype TrieCursor [^BufferAllocator allocator, ^Iterator merge-tasks, ^IRelationWriter out-rel
                     col-names, ^Map col-preds,
                     ^TemporalBounds temporal-bounds
                     params, vsr-cache, buffer-pool]
  ICursor
  (tryAdvance [_ c]
    (if (.hasNext merge-tasks)
      (let [{:keys [leaves path]} (.next merge-tasks)
            is-valid-ptr (ArrowBufPointer.)]
        (reset-vsr-cache vsr-cache)
        (with-open [out-rel (vw/->rel-writer allocator)]
          (let [^IRelationSelector iid-pred (get col-preds "xt$iid")
                merge-q (PriorityQueue. (Comparator/comparing (util/->jfn #(.ev_ptr ^LeafPointer %)) (EventRowPointer/comparator)))
                calculate-polygon (bitemp/polygon-calculator temporal-bounds)
                bitemp-consumer (->bitemporal-consumer out-rel col-names)
                leaf-rdrs (for [leaf leaves
                                :let [^RelationReader data-rdr (merge-task-data-reader buffer-pool vsr-cache leaf)]]
                            (cond-> data-rdr
                              iid-pred (.select (.select iid-pred allocator data-rdr params))))
                [temporal-cols content-cols] ((juxt filter remove) temporal-column? col-names)
                content-rel-factory (->content-rel-factory leaf-rdrs allocator content-cols)]

            (doseq [[idx leaf-rdr] (map-indexed vector leaf-rdrs)
                    :let [ev-ptr (EventRowPointer. leaf-rdr path)]]
              (when (.isValid ev-ptr is-valid-ptr path)
                (.add merge-q (->LeafPointer ev-ptr idx))))

            (loop []
              (when-let [^LeafPointer q-obj (.poll merge-q)]
                (let [^EventRowPointer ev-ptr (.ev_ptr q-obj)]
                  (when-let [^Polygon polygon (calculate-polygon ev-ptr)]
                    (when (= :put (.getOp ev-ptr))
                      (let [sys-from (.getSystemFrom ev-ptr)
                            idx (.getIndex ev-ptr)]
                        (dotimes [i (.getValidTimeRangeCount polygon)]
                          (let [valid-from (.getValidFrom polygon i)
                                valid-to (.getValidTo polygon i)
                                sys-to (.getSystemTo polygon i)]
                            (when (and (.inRange temporal-bounds valid-from valid-to sys-from sys-to)
                                       (not (= valid-from valid-to))
                                       (not (= sys-from sys-to)))
                              (.startRow out-rel)
                              (.accept content-rel-factory (.rel-idx q-obj) idx)
                              (.accept bitemp-consumer idx valid-from valid-to sys-from sys-to)
                              (.endRow out-rel)))))))

                  (.nextIndex ev-ptr)
                  (when (.isValid ev-ptr is-valid-ptr path)
                    (.add merge-q q-obj))
                  (recur))))

            (let [^RelationReader rel (cond-> (.realize content-rel-factory)
                                        (or (empty? (seq content-cols)) (seq temporal-cols))
                                        (vr/concat-rels (vw/rel-wtr->rdr out-rel)))
                  ^RelationReader rel (reduce (fn [^RelationReader rel ^IRelationSelector col-pred]
                                                (.select rel (.select col-pred allocator rel params)))
                                              rel
                                              (vals (dissoc col-preds "xt$iid")))]
              (.accept c rel))))
        true)

      false))

  (close [_]
    (util/close vsr-cache)
    (util/close out-rel)))

(defn- eid-select->eid [eid-select]
  (cond (= 'xt$id (second eid-select))
        (nth eid-select 2)

        (= 'xt$id (nth eid-select 2))
        (second eid-select)))

(defn selects->iid-byte-buffer ^ByteBuffer [selects ^RelationReader params-rel]
  (when-let [eid-select (get selects "xt$id")]
    (when (= '= (first eid-select))
      (when-let [eid (eid-select->eid eid-select)]
        (cond
          (s/valid? ::lp/value eid)
          (trie/->iid eid)

          (s/valid? ::lp/param eid)
          (let [eid-rdr (.readerForName params-rel (name eid))]
            (when (= 1 (.valueCount eid-rdr))
              (trie/->iid (.getObject eid-rdr 0)))))))))

(defn filter-pushdown-bloom-page-idx-pred ^IntPredicate [^ITableMetadata table-metadata ^String col-name]
  (when-let [^MutableRoaringBitmap pushdown-bloom (get *column->pushdown-bloom* (symbol col-name))]
    (let [metadata-rdr (.metadataReader table-metadata)
          bloom-rdr (-> (.structKeyReader metadata-rdr "columns")
                        (.listElementReader)
                        (.structKeyReader "bloom"))]
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

(defn- ->merge-tasks
  "segments :: [Segment]
    Segment :: {:keys [meta-file data-file-path table-metadata page-idx-pred]} ;; for Arrow tries
             | {:keys [trie live-rel]} ;; for live tries

   return :: (seq {:keys [path leaves]})"
  [segments path-pred]
  (let [result (ArrayList.)]
    (->> (HashTrieKt/toMergePlan segments path-pred)
         (run! (fn [^MergePlanTask merge-plan-task]
                 (let [path (.getPath merge-plan-task)
                       mp-nodes (.getMpNodes merge-plan-task)
                       ^MutableRoaringBitmap cumulative-iid-bitmap (MutableRoaringBitmap.)
                       leaves (ArrayList.)]
                   (loop [[^MergePlanNode mp-node & more-mp-nodes] mp-nodes
                          node-taken? false]
                     (if mp-node
                       (let [segment (.getSegment mp-node)
                             trie-node (.getNode mp-node)]
                         (if-not trie-node
                           (recur more-mp-nodes node-taken?)

                           (condp = (class trie-node)
                             ArrowHashTrie$Leaf
                             (let [{:keys [^IntPredicate page-idx-pred ^ITableMetadata table-metadata]} segment
                                   page-idx (.getDataPageIndex ^ArrowHashTrie$Leaf trie-node)
                                   take-node? (.test page-idx-pred page-idx)]

                               (when take-node?
                                 (when-let [iid-bitmap (.iidBloomBitmap table-metadata page-idx)]
                                   (.or cumulative-iid-bitmap iid-bitmap)))

                               (when (or take-node?
                                         (when node-taken?
                                           (when-let [iid-bitmap (.iidBloomBitmap table-metadata page-idx)]
                                             (MutableRoaringBitmap/intersects cumulative-iid-bitmap iid-bitmap))))
                                 (.add leaves [:arrow segment page-idx]))

                               (recur more-mp-nodes (or node-taken? take-node?)))

                             LiveHashTrie$Leaf
                             (let [^LiveHashTrie$Leaf trie-node trie-node
                                   {:keys [^RelationReader live-rel, trie]} segment]
                               (.add leaves [:live (.select live-rel (.mergeSort trie-node trie))])
                               (recur more-mp-nodes true)))))

                       (when node-taken?
                         (.add result {:path path, :leaves (vec leaves)}))))))))
    result))

(defmethod ig/prep-key ::scan-emitter [_ opts]
  (merge opts
         {:metadata-mgr (ig/ref ::meta/metadata-manager)
          :buffer-pool (ig/ref :xtdb/buffer-pool)}))

(defmethod ig/init-key ::scan-emitter [_ {:keys [^IMetadataManager metadata-mgr, ^IBufferPool buffer-pool]}]
  (reify IScanEmitter
    (tableColNames [_ wm table-name]
      (into #{} cat [(keys (.columnFields metadata-mgr table-name))
                     (some-> (.liveIndex wm)
                             (.liveTable table-name)
                             (.columnFields)
                             keys)]))

    (allTableColNames [_ wm]
      (merge-with set/union
                  (update-vals (.allColumnFields metadata-mgr)
                               (comp set keys))
                  (update-vals (some-> (.liveIndex wm)
                                       (.allColumnFields))
                               (comp set keys))))

    (scanFields [_ wm scan-cols]
      (letfn [(->field [[table col-name]]
                (let [normal-table (util/str->normal-form-str (str table))
                      col-name (str col-name)
                      normal-col-name (util/str->normal-form-str col-name)]

                  ;; TODO move to fields here
                  (-> (cond
                        (= "xt$iid" normal-col-name) (types/col-type->field col-name [:fixed-size-binary 16])
                        (types/temporal-column? normal-col-name) (types/col-type->field col-name [:timestamp-tz :micro "UTC"])

                        :else (if-let [info-field (get-in info-schema/derived-tables [normal-table normal-col-name])]
                                info-field
                                (types/merge-fields (.columnField metadata-mgr normal-table normal-col-name)
                                                    (some-> (.liveIndex wm)
                                                            (.liveTable normal-table)
                                                            (.columnField normal-col-name)))))
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
                                                (expr/->expression-relation-selector (-> (edn/parse-expr select-form)
                                                                                         (expr/<-Expr input-types))
                                                                                     input-types))))
                           (into {}))

            metadata-args (vec (for [[col-name select] selects
                                     :when (not (types/temporal-column? col-name))]
                                 select))

            row-count (->> (for [{:keys [tables]} (vals (.chunksMetadata metadata-mgr))
                                 :let [{:keys [row-count]} (get tables table-name)]
                                 :when row-count]
                             row-count)
                           (reduce +))]

        {:fields fields
         :stats {:row-count row-count}
         :->cursor (fn [{:keys [allocator, ^Watermark watermark, basis, params default-all-valid-time?]}]
                     (if-let [derived-table-schema (info-schema/derived-tables table-name)]
                       (info-schema/->cursor allocator derived-table-schema table-name col-names col-preds params metadata-mgr watermark)

                       (let [iid-bb (selects->iid-byte-buffer selects params)
                             col-preds (cond-> col-preds
                                         iid-bb (assoc "xt$iid" (iid-selector iid-bb)))
                             metadata-pred (expr.meta/->metadata-selector (cons 'and metadata-args) (update-vals fields types/field->col-type) params)
                             scan-opts (-> scan-opts
                                           (update :for-valid-time
                                                   (fn [fvt]
                                                     (or fvt (if default-all-valid-time? [:all-time] [:at [:now :now]])))))
                             ^ILiveTableWatermark live-table-wm (some-> (.liveIndex watermark) (.liveTable table-name))
                             table-path (util/table-name->table-path table-name)
                             current-meta-files (->> (trie/list-meta-files buffer-pool table-path)
                                                     (trie/current-trie-files))]

                         (util/with-open [iid-arrow-buf (when iid-bb (util/->arrow-buf-view allocator iid-bb))]
                           (let [merge-tasks (util/with-open [table-metadatas (LinkedList.)]
                                               (or (->merge-tasks (cond-> (mapv (fn [meta-file-path]
                                                                                  (let [{:keys [trie] :as table-metadata} (.openTableMetadata metadata-mgr meta-file-path)]
                                                                                    (.add table-metadatas table-metadata)
                                                                                    (into (trie/->Segment trie)
                                                                                          {:data-file-path (trie/->table-data-file-path table-path (:trie-key (trie/parse-trie-file-path meta-file-path)))

                                                                                           :table-metadata table-metadata
                                                                                           :page-idx-pred (reduce (fn [^IntPredicate page-idx-pred col-name]
                                                                                                                    (if-let [bloom-page-idx-pred (filter-pushdown-bloom-page-idx-pred table-metadata col-name)]
                                                                                                                      (.and page-idx-pred bloom-page-idx-pred)
                                                                                                                      page-idx-pred))
                                                                                                                  (.build metadata-pred table-metadata)
                                                                                                                  col-names)})))
                                                                                current-meta-files)

                                                                    live-table-wm (conj (-> (trie/->Segment (.liveTrie live-table-wm))
                                                                                            (assoc :live-rel (.liveRelation live-table-wm)))))

                                                                  (->path-pred iid-arrow-buf))
                                                   []))]

                             (util/with-close-on-catch [out-rel (RelationWriter. allocator
                                                                                 (for [^Field field (vals fields)]
                                                                                   (vw/->writer (.createVector field allocator))))]
                               (->TrieCursor allocator (.iterator ^Iterable merge-tasks) out-rel
                                             col-names col-preds
                                             (->temporal-bounds params basis scan-opts)
                                             params
                                             (->vsr-cache buffer-pool allocator)
                                             buffer-pool)))))))}))))

(defmethod lp/emit-expr :scan [scan-expr {:keys [^IScanEmitter scan-emitter scan-fields, param-fields]}]
  (.emitScan scan-emitter scan-expr scan-fields param-fields))
