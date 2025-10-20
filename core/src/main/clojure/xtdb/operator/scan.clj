(ns xtdb.operator.scan
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
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
           java.time.Instant
           (java.util LinkedList SortedSet TreeSet)
           (java.util.function IntPredicate Predicate)
           (org.apache.arrow.memory BufferAllocator)
           [org.roaringbitmap.buffer MutableRoaringBitmap]
           xtdb.arrow.RelationReader
           (xtdb.bloom BloomUtils)
           xtdb.catalog.TableCatalog
           xtdb.database.Database$Catalog
           (xtdb ICursor Bytes)
           (xtdb.indexer Snapshot Snapshot$Source)
           (xtdb.metadata MetadataPredicate PageMetadata PageMetadata$Factory)
           (xtdb.operator.scan MultiIidSelector ScanCursor SingleIidSelector)
           (xtdb.segment BufferPoolSegment MemorySegment MergePlanner MergeTask)
           (xtdb.storage BufferPool)
           xtdb.table.TableRef
           (xtdb.trie Bucketer TrieCatalog)
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
  (emitScan [^xtdb.database.Database$Catalog db-cat scan-expr scan-fields param-fields]))

(defn ->scan-cols [{:keys [columns], {:keys [table]} :scan-opts}]
  (for [[col-tag col-arg] columns]
    [table
     (case col-tag
       :column col-arg
       :select (key (first col-arg)))]))

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

(def ^:private dummy-iid (byte-array 16))

(defn selects->iid-bytes ^bytes [selects ^RelationReader args-rel]
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

(defn filter-pushdown-bloom-page-idx-pred ^IntPredicate [^PageMetadata page-metadata, pushdown-blooms, ^String col-name]
  (when-let [^MutableRoaringBitmap pushdown-bloom (get pushdown-blooms (symbol col-name))]
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


(defn ->path-pred [^SortedSet iid-set]
  (when (and iid-set (= 1 (.size iid-set)))
    ;; TODO use the whole set
    (let [^bytes iid-bytes (first iid-set)]
      (reify Predicate
        (test [_ path]
          (zero? (.compareToPath Bucketer/DEFAULT iid-bytes ^bytes path)))))))

(defmethod ig/prep-key ::scan-emitter [_ opts]
  (merge opts
         {:allocator (ig/ref :xtdb/allocator)
          :info-schema (ig/ref :xtdb/information-schema)}))

(defn scan-fields [^Database$Catalog db-catalog, snaps, scan-cols]
  (letfn [(->field [[^TableRef table col-name]]
            (let [col-name (str col-name)]
              ;; TODO move to fields here
              (-> (or (some-> (types/temporal-col-types col-name) types/col-type->field)
                      (-> (info-schema/derived-table table)
                          (get (symbol col-name)))
                      (-> (info-schema/template-table table)
                          (get (symbol col-name)))
                      (let [db-name (.getDbName table)
                            ^TableCatalog table-catalog (.getTableCatalog (.databaseOrNull db-catalog db-name))
                            ^Snapshot snap (get snaps db-name)]
                        (types/merge-fields (.getField table-catalog table col-name)
                                            (some-> (.getLiveIndex snap)
                                                    (.liveTable table)
                                                    (.columnField col-name)))))
                  (types/field-with-name col-name))))]
    (->> scan-cols
         (into {} (map (juxt identity ->field))))))

(defmethod ig/init-key ::scan-emitter [_ {:keys [info-schema]}]
  (reify IScanEmitter
    (emitScan [_ db-cat {:keys [columns], {:keys [^TableRef table] :as scan-opts} :scan-opts} scan-fields param-fields]
      (let [db-name (.getDbName table)
            db (.databaseOrNull db-cat db-name)
            ^PageMetadata$Factory metadata-mgr (.getMetadataManager db)
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

        {:op :scan
         :children []
         :explain {:table (->> [(.getDbName table) (.getSchemaName table) (.getTableName table)]
                               (remove nil?)
                               (str/join "."))
                   :columns (vec col-names)
                   :predicates (mapv pr-str (vals selects))}

         :fields fields
         :stats {:row-count row-count}
         :->cursor (fn [{:keys [allocator, snaps, snapshot-token, schema, args pushdown-blooms pushdown-iids explain-analyze?]}]
                     (let [^Snapshot snapshot (get snaps db-name)
                           derived-table-schema (info-schema/derived-table table)
                           template-table? (boolean (info-schema/template-table table))]
                       (if (and derived-table-schema (not template-table?))
                         (info-schema/->cursor info-schema allocator db snapshot derived-table-schema table col-names col-preds schema args)

                         (let [iid-set (or (when-let [bytes (selects->iid-bytes selects args)]
                                             (doto (TreeSet. Bytes/COMPARATOR)
                                               (.add bytes)))
                                           (get pushdown-iids '_iid) ; usually patch
                                           (get pushdown-iids '_id)) ; any other foreign-key join
                               col-preds (cond-> col-preds
                                           (not (empty? iid-set))
                                           (assoc "_iid" (if (= 1 (count iid-set))
                                                           (SingleIidSelector. (first iid-set))
                                                           (MultiIidSelector. iid-set))))
                               metadata-pred (expr.meta/->metadata-selector allocator (cons 'and metadata-args) (update-vals fields types/field->col-type) args)
                               metadata-pred (reify MetadataPredicate
                                               (build [_ page-metadata]
                                                 (-> (.build metadata-pred page-metadata)
                                                     (as-> pred (reduce (fn [^IntPredicate page-idx-pred col-name]
                                                                          (if-let [bloom-page-idx-pred (filter-pushdown-bloom-page-idx-pred page-metadata pushdown-blooms col-name)]
                                                                            (.and page-idx-pred bloom-page-idx-pred)
                                                                            page-idx-pred))
                                                                        pred
                                                                        col-names)))))
                               scan-opts (-> scan-opts
                                             (update :for-valid-time
                                                     (fn [fvt]
                                                       (or fvt [:at [:now]]))))
                               live-table-snap (some-> (.getLiveIndex snapshot) (.liveTable table))
                               temporal-bounds (->temporal-bounds allocator args scan-opts
                                                                  (-> (basis/<-time-basis-str snapshot-token)
                                                                      (get-in [(.getName db) 0])))]

                           (util/with-close-on-catch [!segments (LinkedList.)]
                             
                             (doseq [{:keys [^String trie-key]} (-> (cat/trie-state trie-catalog table)
                                                                    (cat/current-tries)
                                                                    (cat/filter-tries temporal-bounds))]
                               (.add !segments
                                     (BufferPoolSegment/open allocator buffer-pool metadata-mgr table trie-key metadata-pred)))

                             (when live-table-snap
                               (.add !segments
                                     (MemorySegment. (.getLiveTrie live-table-snap) (.getLiveRelation live-table-snap))))

                             (when template-table?
                               (.add !segments
                                     (let [[memory-rel trie] (info-schema/table-template info-schema table)]
                                       (MemorySegment. trie memory-rel))))

                             (let [merge-tasks (->> (MergePlanner/plan !segments (->path-pred iid-set))
                                                    (into [] (keep (fn [^MergeTask mt]
                                                                     (when-let [pages (trie/filter-pages (.getPages mt) temporal-bounds)]
                                                                       (MergeTask. pages (.getPath mt)))))))]
                               (cond-> (ScanCursor. allocator col-names col-preds
                                                    temporal-bounds
                                                    !segments (.iterator ^Iterable merge-tasks)
                                                    schema args)
                                 explain-analyze? (ICursor/wrapExplainAnalyze))))))))}))))

(defmethod lp/emit-expr :scan [scan-expr {:keys [^IScanEmitter scan-emitter db-cat scan-fields, param-fields]}]
  (assert db-cat)
  (.emitScan scan-emitter db-cat scan-expr scan-fields param-fields))
