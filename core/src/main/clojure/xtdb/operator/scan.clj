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
            [xtdb.logical-plan :as lp]
            [xtdb.metadata :as meta]
            xtdb.object-store
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang MapEntry)
           (java.io Closeable)
           java.nio.ByteBuffer
           (java.nio.file Path)
           (java.util Comparator HashMap Iterator LinkedList Map PriorityQueue)
           (java.util.function IntPredicate Predicate)
           (java.util.stream IntStream)
           (org.apache.arrow.memory ArrowBuf BufferAllocator)
           [org.apache.arrow.memory.util ArrowBufPointer]
           (org.apache.arrow.vector VectorLoader)
           (org.apache.arrow.vector.types.pojo FieldType)
           [org.roaringbitmap.buffer MutableRoaringBitmap]
           xtdb.IBufferPool
           xtdb.ICursor
           xtdb.api.TransactionKey
           (xtdb.bitemporal IRowConsumer Polygon)
           (xtdb.metadata IMetadataManager ITableMetadata)
           xtdb.operator.IRelationSelector
           (xtdb.trie ArrowLeaf LiveLeaf MergeTask HashTrieKt EventRowPointer HashTrie ArrowSegment LiveSegment)
           (xtdb.util TemporalBounds TemporalBounds$TemporalColumn)
           (xtdb.vector IRelationWriter IRowCopier IVectorReader IVectorWriter RelationReader)
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

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn- ->content-consumer [^IRelationWriter out-rel, ^RelationReader leaf-rel, col-names]
  (let [op-rdr (.readerForName leaf-rel "op")
        put-rdr (.legReader op-rdr :put)

        row-copiers (object-array
                     (for [^String col-name col-names
                           :let [normalized-name (util/str->normal-form-str col-name)
                                 copier (case normalized-name
                                          "xt$iid"
                                          (.rowCopier (.readerForName leaf-rel "xt$iid")
                                                      (.colWriter out-rel col-name (FieldType/notNullable (types/->arrow-type [:fixed-size-binary 16]))))
                                          ("xt$system_from" "xt$system_to" "xt$valid_from" "xt$valid_to") nil
                                          (some-> (.structKeyReader put-rdr normalized-name)
                                                  (.rowCopier (.colWriter out-rel col-name))))]
                           :when copier]
                       copier))]

    (reify IRowConsumer
      (accept [_ idx _valid-from _valid-to _sys-from _sys-to]
        (dotimes [i (alength row-copiers)]
          (let [^IRowCopier copier (aget row-copiers i)]
            (.copyRow copier idx)))))))

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

(defrecord VSRCache [^IBufferPool buffer-pool, ^BufferAllocator allocator, ^Map cache]
  Closeable
  (close [_] (util/close cache)))

(defn ->vsr-cache [buffer-pool allocator]
  (->VSRCache buffer-pool allocator (HashMap.)))

(defn cache-vsr [{:keys [^Map cache, buffer-pool, allocator]} ^Path trie-leaf-file]
  (.computeIfAbsent cache trie-leaf-file
                    (util/->jfn
                      (fn [trie-leaf-file]
                        (bp/open-vsr buffer-pool trie-leaf-file allocator)))))

(defn merge-task-data-reader ^IVectorReader [buffer-pool vsr-cache ^Path table-path leaf]
  (condp = (class leaf)
    ArrowLeaf
    (let [^ArrowSegment arrow-segment (.getSegment ^ArrowLeaf leaf)
          trie-key (.getTrieKey arrow-segment)
          page-idx (.getPageIdx ^ArrowLeaf leaf)
          data-file-path (trie/->table-data-file-path table-path trie-key)]
      (util/with-open [rb (bp/open-record-batch buffer-pool data-file-path page-idx)]
        (let [vsr (cache-vsr vsr-cache data-file-path)
              loader (VectorLoader. vsr)]
          (.load loader rb)
          (vr/<-root vsr))))

    LiveLeaf
    (.getLiveRel ^LiveLeaf leaf)))

(deftype TrieCursor [^BufferAllocator allocator, ^Iterator merge-tasks
                     ^Path table-path, col-names, ^Map col-preds,
                     ^TemporalBounds temporal-bounds
                     params, vsr-cache, buffer-pool]
  ICursor
  (tryAdvance [_ c]
    (if (.hasNext merge-tasks)
      (let [^MergeTask merge-task (.next merge-tasks)
            leaves (.getLeaves merge-task)
            path (.getPath merge-task)
            is-valid-ptr (ArrowBufPointer.)]
        (with-open [out-rel (vw/->rel-writer allocator)]
          (let [^IRelationSelector iid-pred (get col-preds "xt$iid")
                merge-q (PriorityQueue. (Comparator/comparing (util/->jfn :ev-ptr) (EventRowPointer/comparator)))
                calculate-polygon (bitemp/polygon-calculator temporal-bounds)
                bitemp-consumer (->bitemporal-consumer out-rel col-names)]

            (doseq [leaf leaves
                    :let [^RelationReader data-rdr (merge-task-data-reader buffer-pool vsr-cache table-path leaf)
                          ^RelationReader leaf-rdr (cond-> data-rdr
                                                     iid-pred (.select (.select iid-pred allocator data-rdr params)))
                          ev-ptr (EventRowPointer. leaf-rdr path)]]
              (when (.isValid ev-ptr is-valid-ptr path)
                (.add merge-q {:ev-ptr ev-ptr, :content-consumer (->content-consumer out-rel leaf-rdr col-names)})))

            (loop []
              (when-let [{:keys [^EventRowPointer ev-ptr, ^IRowConsumer content-consumer] :as q-obj} (.poll merge-q)]
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
                            (.accept content-consumer idx valid-from valid-to sys-from sys-to)
                            (.accept bitemp-consumer idx valid-from valid-to sys-from sys-to)
                            (.endRow out-rel)))))))

                (.nextIndex ev-ptr)
                (when (.isValid ev-ptr is-valid-ptr path)
                  (.add merge-q q-obj))
                (recur)))

            (let [^RelationReader rel (reduce (fn [^RelationReader rel ^IRelationSelector col-pred]
                                                (.select rel (.select col-pred allocator rel params)))
                                              (-> (vw/rel-wtr->rdr out-rel)
                                                  (vr/with-absent-cols allocator col-names))
                                              (vals (dissoc col-preds "xt$iid")))]
              (.accept c rel))))
        true)

      false))

  (close [_]
    (util/close vsr-cache)))

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
           (when-let [bloom-vec-idx (.rowIndex table-metadata col-name page-idx)]
             (and (not (nil? (.getObject bloom-rdr bloom-vec-idx)))
                  (MutableRoaringBitmap/intersects pushdown-bloom
                                                   (bloom/bloom->bitmap bloom-rdr bloom-vec-idx))))))))))

(defn- ->path-pred [^ArrowBuf iid-arrow-buf]
  (if iid-arrow-buf
    (let [iid-ptr (ArrowBufPointer. iid-arrow-buf 0 (.capacity iid-arrow-buf))]
      (reify Predicate
        (test [_ path]
          (zero? (HashTrie/compareToPath iid-ptr path)))))
    (reify Predicate
      (test [_ _path]
        true))))

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
                                       (.allColumnFields ))
                               (comp set keys))))

    (scanFields [_ wm scan-cols]
      (letfn [(->field [[table col-name]]
                (let [table (str table)
                      col-name (str col-name)]

                  ;; TODO move to fields here
                  (cond
                    (= "xt$iid" col-name) (types/col-type->field col-name [:fixed-size-binary 16])
                    (types/temporal-column? col-name) (types/col-type->field col-name [:timestamp-tz :micro "UTC"])

                    :else (types/merge-fields (.columnField metadata-mgr table col-name)
                                              (some-> (.liveIndex wm)
                                                      (.liveTable table)
                                                      (.columnFields)
                                                      (get col-name))))))]
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
                                                (expr/->expression-relation-selector (expr/form->expr select-form input-types)
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
                                             (HashTrieKt/toMergeTasks
                                              (cond-> (mapv (fn [meta-file-path]
                                                              (let [{:keys [trie] :as table-metadata} (.openTableMetadata metadata-mgr meta-file-path)]
                                                                (.add table-metadatas table-metadata)
                                                                (ArrowSegment. table-metadata
                                                                               (reduce (fn [^IntPredicate page-idx-pred col-name]
                                                                                         (if-let [bloom-page-idx-pred (filter-pushdown-bloom-page-idx-pred table-metadata col-name)]
                                                                                           (.and page-idx-pred bloom-page-idx-pred)
                                                                                           page-idx-pred))
                                                                                       (.build metadata-pred table-metadata)
                                                                                       col-names)
                                                                               (:trie-key (trie/parse-trie-file-path meta-file-path))
                                                                               trie)))
                                                            current-meta-files)

                                                live-table-wm
                                                (conj (LiveSegment. (.liveRelation live-table-wm) (.liveTrie live-table-wm))))

                                              (->path-pred iid-arrow-buf)))]

                           (->TrieCursor allocator (.iterator ^Iterable merge-tasks)
                                         table-path col-names col-preds
                                         (->temporal-bounds params basis scan-opts)
                                         params
                                         (->vsr-cache buffer-pool allocator)
                                         buffer-pool)))))}))))

(defmethod lp/emit-expr :scan [scan-expr {:keys [^IScanEmitter scan-emitter scan-fields, param-fields]}]
  (.emitScan scan-emitter scan-expr scan-fields param-fields))
