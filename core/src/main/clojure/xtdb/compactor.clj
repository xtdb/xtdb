(ns xtdb.compactor
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.bitemporal :as bitemp]
            [xtdb.metrics :as metrics]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import [java.nio.channels ClosedByInterruptException]
           [java.util ArrayList Arrays Comparator LinkedList PriorityQueue]
           (java.util.function Predicate)
           [org.apache.arrow.memory BufferAllocator]
           [org.apache.arrow.memory.util ArrowBufPointer]
           (org.apache.arrow.vector.types.pojo Field FieldType)
           (xtdb BufferPool)
           xtdb.api.CompactorConfig
           (xtdb.api.log Log)
           (xtdb.arrow Relation RelationReader RowCopier Vector VectorWriter)
           xtdb.bitemporal.IPolygonReader
           (xtdb.compactor Compactor Compactor$Impl Compactor$Job)
           xtdb.BufferPool
           (xtdb.metadata IMetadataManager)
           (xtdb.trie EventRowPointer EventRowPointer$XtArrow HashTrieKt IDataRel MergePlanTask TrieCatalog)
           (xtdb.util TemporalBounds)))

(def ^:dynamic *ignore-signal-block?* false)

(defn- ->reader->copier [^Relation data-wtr]
  (let [iid-wtr (.get data-wtr "_iid")
        sf-wtr (.get data-wtr "_system_from")
        vf-wtr (.get data-wtr "_valid_from")
        vt-wtr (.get data-wtr "_valid_to")
        op-wtr (.get data-wtr "op")]
    (fn reader->copier [^RelationReader data-rdr]
      (let [iid-copier (-> (.get data-rdr "_iid") (.rowCopier iid-wtr))
            sf-copier (-> (.get data-rdr "_system_from") (.rowCopier sf-wtr))
            vf-copier (-> (.get data-rdr "_valid_from") (.rowCopier vf-wtr))
            vt-copier (-> (.get data-rdr "_valid_to") (.rowCopier vt-wtr))
            op-copier (-> (.get data-rdr "op") (.rowCopier op-wtr))]
        (reify RowCopier
          (copyRow [_ ev-idx]
            (let [pos (.copyRow iid-copier ev-idx)]
              (.copyRow sf-copier ev-idx)
              (.copyRow vf-copier ev-idx)
              (.copyRow vt-copier ev-idx)
              (.copyRow op-copier ev-idx)
              (.endRow data-wtr)

              pos)))))))

(defn merge-segments-into [^Relation data-rel, ^VectorWriter recency-wtr, segments, ^bytes path-filter]
  (let [reader->copier (->reader->copier data-rel)
        calculate-polygon (bitemp/polygon-calculator)

        is-valid-ptr (ArrowBufPointer.)]

    (doseq [^MergePlanTask merge-plan-task (HashTrieKt/toMergePlan segments
                                                                   (when path-filter
                                                                     (let [path-len (alength path-filter)]
                                                                       (reify Predicate
                                                                         (test [_ page-path]
                                                                           (let [^bytes page-path page-path
                                                                                 len (min path-len (alength page-path))]
                                                                             (Arrays/equals path-filter 0 len
                                                                                            page-path 0 len))))))
                                                                   (TemporalBounds.))
            :let [_ (when (Thread/interrupted)
                      (throw (InterruptedException.)))

                  mp-nodes (.getMpNodes merge-plan-task)
                  ^bytes path (.getPath merge-plan-task)
                  data-rdrs (mapv trie/load-data-page mp-nodes)
                  merge-q (PriorityQueue. (Comparator/comparing :ev-ptr (EventRowPointer/comparator)))
                  path (if (or (nil? path-filter)
                               (> (alength path) (alength path-filter)))
                         path
                         path-filter)]]

      (doseq [^RelationReader data-rdr data-rdrs
              :when data-rdr
              :let [ev-ptr (EventRowPointer$XtArrow. data-rdr path)
                    row-copier (reader->copier data-rdr)]]
        (when (.isValid ev-ptr is-valid-ptr path)
          (.add merge-q {:ev-ptr ev-ptr, :row-copier row-copier})))

      (loop [seen-erase? false]
        (when-let [{:keys [^EventRowPointer ev-ptr, ^RowCopier row-copier] :as q-obj} (.poll merge-q)]
          (let [new-previous-polygon  (if-let [polygon (calculate-polygon ev-ptr)]
                                        (do
                                          (.copyRow row-copier (.getIndex ev-ptr))
                                          (.writeLong recency-wtr (.getRecency ^IPolygonReader polygon))
                                          false)

                                        (do
                                          ;; the first time we encounter an erase
                                          (when-not seen-erase?
                                            (.copyRow row-copier (.getIndex ev-ptr))
                                            ;; TODO this can likely become system-time, but we wanted to play it safe for now
                                            (.writeLong recency-wtr Long/MAX_VALUE))
                                          true))]
            (.nextIndex ev-ptr)
            (when (.isValid ev-ptr is-valid-ptr path)
              (.add merge-q q-obj))
            (recur new-previous-polygon)))))

    nil))

(defn ->log-data-rel-schema ^org.apache.arrow.vector.types.pojo.Schema [data-rels]
  (trie/data-rel-schema (-> (for [^IDataRel data-rel data-rels]
                                   (-> (.getSchema data-rel)
                                       (.findField "op")
                                       (.getChildren) ^Field first))
                            (->> (apply types/merge-fields))
                            (types/field-with-name "put"))))

(defn open-recency-wtr ^xtdb.arrow.Vector [allocator]
  (Vector/fromField allocator
                    (Field. "_recency"
                            (FieldType/notNullable #xt.arrow/type [:timestamp-tz :micro "UTC"])
                            nil)))

(defn exec-compaction-job! [^BufferAllocator allocator, ^BufferPool buffer-pool, ^IMetadataManager metadata-mgr
                            {:keys [page-size]} {:keys [table-name part trie-keys out-trie-key]}]
  (try
    (log/debugf "compacting '%s' '%s' -> '%s'..." table-name trie-keys out-trie-key)

    (util/with-open [table-metadatas (LinkedList.)
                     data-rels (trie/open-data-rels buffer-pool table-name trie-keys)]
      (doseq [trie-key trie-keys]
        (.add table-metadatas (.openTableMetadata metadata-mgr (trie/->table-meta-file-path table-name trie-key))))

      (let [segments (mapv (fn [{:keys [trie] :as _table-metadata} data-rel]
                             (-> (trie/->Segment trie) (assoc :data-rel data-rel)))
                           table-metadatas
                           data-rels)
            schema (->log-data-rel-schema (map :data-rel segments))]

        (util/with-open [data-rel (Relation. allocator schema)
                         recency-wtr (open-recency-wtr allocator)]
          (merge-segments-into data-rel recency-wtr segments part)

          (util/with-open [trie-wtr (trie/open-trie-writer allocator buffer-pool
                                                           schema table-name out-trie-key
                                                           true)]

            (Compactor/writeRelation trie-wtr data-rel recency-wtr page-size)))))

    (log/debugf "compacted '%s' -> '%s'." table-name out-trie-key)

    (catch ClosedByInterruptException _ (throw (InterruptedException.)))
    (catch InterruptedException e (throw e))

    (catch Throwable t
      (log/error t "Error running compaction job.")
      (throw t))))

(defrecord Job [table-name trie-keys part out-trie-key]
  Compactor$Job
  (getTableName [_] table-name)
  (getOutputTrieKey [_] out-trie-key))

(defn- l0->l1-compaction-job [table-name {l0-trie-keys 0, l1-trie-keys 1} {:keys [^long l1-file-size-rows]}]
  (let [last-l1-file (last l1-trie-keys)
        l1-compacted-row (long (if-let [{:keys [^long next-row]} last-l1-file]
                                 next-row
                                 -1))]

    (when-let [current-l0-trie-keys (seq (->> l0-trie-keys
                                              (drop-while (fn [{:keys [^long next-row]}]
                                                            (<= next-row l1-compacted-row)))))]

      ;; if there are current L0 files, merge them into the latest l1 file until it's full
      (let [{:keys [trie-keys ^long first-row ^long next-row ^long rows]}
            (reduce (fn [{:keys [^long rows first-row trie-keys]}
                         {^long l0-rows :rows, l0-first-row :first-row l0-trie-key :trie-key, :keys [^long next-row]}]
                      (let [new-rows (+ rows l0-rows)]
                        (cond-> {:first-row (or first-row l0-first-row)
                                 :rows new-rows
                                 :trie-keys (conj trie-keys l0-trie-key)
                                 :next-row next-row}
                          (>= new-rows l1-file-size-rows) reduced)))

                    (or (when-let [{:keys [^long rows first-row trie-key]} last-l1-file]
                          (when (< rows l1-file-size-rows)
                            {:first-row first-row :rows rows, :trie-keys [trie-key]}))

                        {:rows 0, :trie-keys []})

                    current-l0-trie-keys)]

        (->Job table-name trie-keys nil (trie/->log-l0-l1-trie-key 1 first-row next-row rows))))))

(defn compaction-jobs [table-name meta-file-names {:keys [^long l1-file-size-rows] :as opts}]
  (when (seq meta-file-names)
    (let [!compaction-jobs (ArrayList.)

          level-grouped-file-names (->> meta-file-names
                                        (keep trie/parse-trie-file-path)
                                        (group-by :level))]

      (loop [level (long (last (sort (keys level-grouped-file-names))))
             ^longs !compacted-rows-above (trie/path-array (inc level))]
        (if (zero? level)
          (when-let [job (l0->l1-compaction-job table-name level-grouped-file-names opts)]
            (.add !compaction-jobs job))
            ;; exit `loop`


          (let [!compacted-rows (trie/rows-covered-below !compacted-rows-above)
                lvl-trie-keys (cond->> (get level-grouped-file-names level)
                                (= level 1) (filter (fn [{:keys [^long rows]}]
                                                      (>= rows l1-file-size-rows))))]

            (doseq [[^long path-idx trie-keys] (->> lvl-trie-keys
                                                    (group-by (if (= level 1)
                                                                (constantly 0)
                                                                (comp trie/path-array-idx :part))))

                    :let [min-compacted-row (aget !compacted-rows path-idx)
                          trie-keys (->> trie-keys
                                         (drop-while (fn [{:keys [^long next-row]}]
                                                       (<= next-row min-compacted-row))))]
                    :when (seq trie-keys)]

              (aset !compacted-rows path-idx (max min-compacted-row ^long (:next-row (last trie-keys))))

              (when (= 4 (count (take 4 trie-keys)))
                (dotimes [path-suffix 4]
                  (let [compacted-row (aget !compacted-rows-above (+ (* path-idx 4) path-suffix))
                        trie-keys (->> trie-keys
                                       (drop-while (fn [{:keys [^long next-row]}]
                                                     (<= next-row compacted-row)))
                                       (take 4))]
                    (when (= 4 (count trie-keys))
                      (let [{:keys [part ^long next-row]} (last trie-keys)
                            compaction-part (HashTrieKt/conjPath (or part (byte-array 0)) path-suffix)]
                        (.add !compaction-jobs
                              (->Job table-name (mapv :trie-key trie-keys) compaction-part
                                     (trie/->log-l2+-trie-key (inc level) compaction-part next-row)))))))))

            (recur (dec level) !compacted-rows))))

      (vec !compaction-jobs))))

(defmethod ig/prep-key :xtdb/compactor [_ ^CompactorConfig config]
  {:allocator (ig/ref :xtdb/allocator)
   :buffer-pool (ig/ref :xtdb/buffer-pool)
   :metadata-mgr (ig/ref :xtdb.metadata/metadata-manager)
   :threads (max 1 (/ (.availableProcessors (Runtime/getRuntime)) 2))
   :metrics-registry (ig/ref :xtdb.metrics/registry)
   :log (ig/ref :xtdb/log)
   :trie-catalog (ig/ref :xtdb/trie-catalog)
   :enabled? (.getEnabled config)})

(def ^:dynamic *page-size* 1024)
(def ^:dynamic *l1-file-size-rows* (bit-shift-left 1 18))

(defn- open-compactor [{:keys [allocator, ^BufferPool buffer-pool, ^Log log, ^TrieCatalog trie-catalog, metadata-mgr,
                               ^long threads metrics-registry]}]
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "compactor")]
    (metrics/add-allocator-gauge metrics-registry "compactor.allocator.allocated_memory" allocator)
    (let [page-size *page-size*
          l1-file-size-rows *l1-file-size-rows*]
      (Compactor/open
       (reify Compactor$Impl
         (availableJobs [_]
           (for [table-name (.getTableNames trie-catalog)
                 job (compaction-jobs table-name (trie/list-meta-files buffer-pool table-name)
                                      {:l1-file-size-rows l1-file-size-rows})]
             job))

         (executeJob [_ job]
           (exec-compaction-job! allocator buffer-pool metadata-mgr {:page-size page-size} job)))

       log trie-catalog *ignore-signal-block?* threads))))

(defmethod ig/init-key :xtdb/compactor [_ {:keys [enabled?] :as opts}]
  (if enabled?
    (open-compactor opts)
    (Compactor/getNoop)))

(defmethod ig/halt-key! :xtdb/compactor [_ compactor]
  (util/close compactor))

(defn signal-block! [^Compactor compactor]
  (.signalBlock compactor))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn compact-all!
  ([node] (compact-all! node nil))
  ([node timeout] (.compactAll ^Compactor (util/component node :xtdb/compactor) timeout)))
