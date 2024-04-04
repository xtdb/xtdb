(ns xtdb.compactor
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.bitemporal :as bitemp]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (java.lang AutoCloseable)
           [java.nio.file Path]
           [java.util ArrayList Arrays Comparator HashSet LinkedList PriorityQueue]
           [java.util.concurrent Executors TimeUnit]
           [java.util.concurrent.locks ReentrantLock]
           (java.util.function Predicate)
           [org.apache.arrow.memory BufferAllocator]
           [org.apache.arrow.memory.util ArrowBufPointer]
           (org.apache.arrow.vector.types.pojo Field FieldType)
           (xtdb Compactor IBufferPool)
           xtdb.bitemporal.IPolygonReader
           (xtdb.metadata IMetadataManager)
           (xtdb.trie EventRowPointer HashTrieKt IDataRel MergePlanTask)
           xtdb.vector.IRelationWriter
           xtdb.vector.IRowCopier
           xtdb.vector.IVectorWriter
           xtdb.vector.RelationReader))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ICompactor
  (^void compactAll [])
  (^void signalBlock []))

(defn- ->reader->copier [^IRelationWriter data-wtr]
  (let [iid-wtr (.colWriter data-wtr "xt$iid")
        sf-wtr (.colWriter data-wtr "xt$system_from")
        vf-wtr (.colWriter data-wtr "xt$valid_from")
        vt-wtr (.colWriter data-wtr "xt$valid_to")
        op-wtr (.colWriter data-wtr "op")]
    (fn reader->copier [^RelationReader data-rdr]
      (let [iid-copier (-> (.readerForName data-rdr "xt$iid") (.rowCopier iid-wtr))
            sf-copier (-> (.readerForName data-rdr "xt$system_from") (.rowCopier sf-wtr))
            vf-copier (-> (.readerForName data-rdr "xt$valid_from") (.rowCopier vf-wtr))
            vt-copier (-> (.readerForName data-rdr "xt$valid_to") (.rowCopier vt-wtr))
            op-copier (-> (.readerForName data-rdr "op") (.rowCopier op-wtr))]
        (reify IRowCopier
          (copyRow [_ ev-idx]
            (.startRow data-wtr)
            (let [pos (.copyRow iid-copier ev-idx)]
              (.copyRow sf-copier ev-idx)
              (.copyRow vf-copier ev-idx)
              (.copyRow vt-copier ev-idx)
              (.copyRow op-copier ev-idx)
              (.endRow data-wtr)

              pos)))))))

(defn merge-segments-into [^IRelationWriter data-rel-wtr, ^IVectorWriter recency-wtr, segments, ^bytes path-filter]
  (let [reader->copier (->reader->copier data-rel-wtr)
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
                                                                                            page-path 0 len)))))))
            :let [_ (when (Thread/interrupted)
                      (throw (InterruptedException.)))

                  mp-nodes (.getMpNodes merge-plan-task)
                  ^bytes path (.getPath merge-plan-task)
                  data-rdrs (mapv trie/load-data-page mp-nodes)
                  merge-q (PriorityQueue. (Comparator/comparing (util/->jfn :ev-ptr) (EventRowPointer/comparator)))
                  path (if (or (nil? path-filter)
                               (> (alength path) (alength path-filter)))
                         path
                         path-filter)]]

      (doseq [^RelationReader data-rdr data-rdrs
              :when data-rdr
              :let [ev-ptr (EventRowPointer. data-rdr path)
                    row-copier (reader->copier data-rdr)]]
        (when (.isValid ev-ptr is-valid-ptr path)
          (.add merge-q {:ev-ptr ev-ptr, :row-copier row-copier})))

      (loop []
        (when-let [{:keys [^EventRowPointer ev-ptr, ^IRowCopier row-copier] :as q-obj} (.poll merge-q)]
          (.copyRow row-copier (.getIndex ev-ptr))

          (.writeLong recency-wtr
                      (.getRecency ^IPolygonReader (calculate-polygon ev-ptr)))

          (.nextIndex ev-ptr)
          (when (.isValid ev-ptr is-valid-ptr path)
            (.add merge-q q-obj))
          (recur))))

    nil))

(defn ->log-data-rel-schema [data-rels]
  (trie/data-rel-schema (->> (for [^IDataRel data-rel data-rels]
                               (-> (.getSchema data-rel)
                                   (.findField "op")
                                   (.getChildren) ^Field first
                                   types/field->col-type))
                             (apply types/merge-col-types))))

(defn open-recency-wtr [allocator]
  (vw/->vec-writer allocator "xt$recency"
                   (FieldType/notNullable #xt.arrow/type [:timestamp-tz :micro "UTC"])))

(defn exec-compaction-job! [^BufferAllocator allocator, ^IBufferPool buffer-pool, ^IMetadataManager metadata-mgr
                            {:keys [page-size]} {:keys [^Path table-path path trie-keys out-trie-key]}]
  (try
    (log/debugf "compacting '%s' '%s' -> '%s'..." table-path trie-keys out-trie-key)

    (util/with-open [table-metadatas (LinkedList.)
                     data-rels (trie/open-data-rels buffer-pool table-path trie-keys nil)]
      (doseq [trie-key trie-keys]
        (.add table-metadatas (.openTableMetadata metadata-mgr (trie/->table-meta-file-path table-path trie-key))))

      (let [segments (mapv (fn [{:keys [trie] :as _table-metadata} data-rel]
                             (-> (trie/->Segment trie) (assoc :data-rel data-rel)))
                           table-metadatas
                           data-rels)
            schema (->log-data-rel-schema (map :data-rel segments))]

        (util/with-open [data-rel-wtr (trie/open-log-data-wtr allocator schema)
                         recency-wtr (open-recency-wtr allocator)]
          (merge-segments-into data-rel-wtr recency-wtr segments path)

          (util/with-open [trie-wtr (trie/open-trie-writer allocator buffer-pool
                                                           schema table-path out-trie-key)]

            (Compactor/writeRelation trie-wtr (vw/rel-wtr->rdr data-rel-wtr) (vw/vec-wtr->rdr recency-wtr) page-size)))))

    (log/debugf "compacted '%s' -> '%s'." table-path out-trie-key)

    (catch InterruptedException e (throw e))

    (catch Throwable t
      (log/error t "Error running compaction job.")
      (throw t))))

(defn- l0->l1-compaction-job [{l0-trie-keys 0, l1-trie-keys 1} {:keys [^long l1-file-size-rows]}]
  (let [last-l1-file (last l1-trie-keys)
        l1-compacted-row (long (if-let [{:keys [^long next-row]} last-l1-file]
                                 next-row
                                 -1))]

    (when-let [current-l0-trie-keys (seq (->> l0-trie-keys
                                              (drop-while (fn [{:keys [^long next-row]}]
                                                            (<= next-row l1-compacted-row)))))]

      ;; if there are current L0 files, merge them into the latest l1 file until it's full
      (let [{:keys [trie-keys ^long next-row ^long rows]}
            (reduce (fn [{:keys [^long rows trie-keys]}
                         {^long l0-rows :rows, l0-trie-key :trie-key, :keys [^long next-row]}]
                      (let [new-rows (+ rows l0-rows)]
                        (cond-> {:rows new-rows
                                 :trie-keys (conj trie-keys l0-trie-key)
                                 :next-row next-row}
                          (>= new-rows l1-file-size-rows) reduced)))

                    (or (when-let [{:keys [^long rows trie-key]} last-l1-file]
                          (when (< rows l1-file-size-rows)
                            {:rows rows, :trie-keys [trie-key]}))

                        {:rows 0, :trie-keys []})

                    current-l0-trie-keys)]

        {:trie-keys trie-keys
         :out-trie-key (trie/->log-l0-l1-trie-key 1 next-row rows)}))))

(defn compaction-jobs [meta-file-names {:keys [^long l1-file-size-rows] :as opts}]
  (when (seq meta-file-names)
    (let [!compaction-jobs (ArrayList.)

          level-grouped-file-names (->> meta-file-names
                                        (keep trie/parse-trie-file-path)
                                        (group-by :level))]

      (loop [level (long (last (sort (keys level-grouped-file-names))))
             ^longs !compacted-rows-above (trie/path-array (inc level))]
        (if (zero? level)
          (when-let [job (l0->l1-compaction-job level-grouped-file-names opts)]
            (.add !compaction-jobs job)
            ;; exit `loop`
            )

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
                              {:trie-keys (mapv :trie-key trie-keys)
                               :path compaction-part
                               :out-trie-key (trie/->log-l2+-trie-key (inc level) compaction-part next-row)})))))))

            (recur (dec level) !compacted-rows))))

      (vec !compaction-jobs))))

(defmethod ig/prep-key :xtdb/compactor [_ opts]
  (into {:allocator (ig/ref :xtdb/allocator)
         :buffer-pool (ig/ref :xtdb/buffer-pool)
         :metadata-mgr (ig/ref :xtdb.metadata/metadata-manager)
         :threads (max 1 (/ (.availableProcessors (Runtime/getRuntime)) 2))}
        opts))

(def ^:dynamic *page-size* 1024)
(def ^:dynamic *l1-file-size-rows* (bit-shift-left 1 18))

(defmethod ig/init-key :xtdb/compactor [_ {:keys [allocator, ^IBufferPool buffer-pool, metadata-mgr, ^long threads]}]
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "compactor")]
    (let [page-size *page-size*
          l1-file-size-rows *l1-file-size-rows*]
      (letfn [(available-jobs []
                (for [table-path (.listObjects buffer-pool util/tables-dir)
                      job (compaction-jobs (trie/list-meta-files buffer-pool table-path)
                                           {:l1-file-size-rows l1-file-size-rows})]
                  (assoc job :table-path table-path)))]

        (let [lock (ReentrantLock.)
              job-finished (.newCondition lock)
              compact-requested (.newCondition lock)
              !active-jobs (HashSet.)
              thread-pool (Executors/newThreadPerTaskExecutor (-> (Thread/ofVirtual)
                                                                  (.name "xtdb.compactor-" 0)
                                                                  (.factory)))]

          (letfn [(available-job []
                    (let [jobs (shuffle (available-jobs))]
                      (.lock lock)
                      (try
                        (some (fn [{:keys [out-trie-key] :as job}]
                                (when (.add !active-jobs out-trie-key)
                                  job))
                              jobs)
                        (finally
                          (.unlock lock)))))

                  (compactor-loop []
                    (try
                      (loop []
                        (when (Thread/interrupted)
                          (throw (InterruptedException.)))

                        (if-let [{:keys [out-trie-key] :as job} (available-job)]
                          (do
                            (exec-compaction-job! allocator buffer-pool metadata-mgr {:page-size page-size} job)
                            (.lock lock)
                            (try
                              (.remove !active-jobs out-trie-key)
                              (.signalAll job-finished)
                              (finally
                                (.unlock lock))))

                          (do
                            (.lock lock)
                            (try
                              (.await compact-requested 5 TimeUnit/SECONDS)
                              (catch Throwable t
                                (throw t))
                              (finally
                                (.unlock lock)))))

                        (recur))

                      (catch InterruptedException _)
                      (catch Throwable t
                        (log/warn t "uncaught compactor-loop error"))))]

            (dotimes [_ threads]
              (.execute thread-pool compactor-loop))

            (reify ICompactor
              (compactAll [_]
                (log/info "compact-all")
                (.lock lock)
                (try
                  (.signalAll compact-requested)
                  (finally
                    (.unlock lock)))

                (loop []
                  (.lock lock)
                  (when-not (try
                              (cond
                                (not (.isEmpty !active-jobs)) (do (.await job-finished) false)
                                (not (empty? (available-jobs))) (do (.await job-finished 5 TimeUnit/SECONDS) false)
                                :else true)
                              (finally
                                (.unlock lock)))
                    (recur))))

              (signalBlock [_]
                (.lock lock)
                (try
                  (.signalAll compact-requested)
                  (finally
                    (.unlock lock))))

              AutoCloseable
              (close [_]
                (.shutdownNow thread-pool)
                (when-not (.awaitTermination thread-pool 20 TimeUnit/SECONDS)
                  (log/warn "could not close compaction thread-pool after 20s"))

                (util/close allocator)))))))))

(defmethod ig/halt-key! :xtdb/compactor [_ compactor]
  (util/close compactor))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn compact-all! [node]
  (let [^ICompactor compactor (util/component node :xtdb/compactor)]
    (.compactAll compactor)))
