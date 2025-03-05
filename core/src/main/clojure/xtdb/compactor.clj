(ns xtdb.compactor
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            xtdb.metadata
            [xtdb.metrics :as metrics]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as cat]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import [java.nio.channels ClosedByInterruptException]
           [java.util LinkedList]
           [org.apache.arrow.memory BufferAllocator]
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb BufferPool)
           xtdb.api.CompactorConfig
           (xtdb.api.log Log)
           (xtdb.arrow Relation)
           xtdb.BufferPool
           (xtdb.compactor Compactor Compactor$Impl Compactor$Job)
           (xtdb.metadata PageMetadata PageMetadata$Factory)
           (xtdb.trie DataRel TrieCatalog TrieWriter)))

(def ^:dynamic *ignore-signal-block?* false)

(defn merge-segments-into [^Relation data-rel, segments, ^bytes path-filter]
  (Compactor/mergeSegmentsInto data-rel segments path-filter))

(defn ->log-data-rel-schema ^org.apache.arrow.vector.types.pojo.Schema [data-rels]
  (trie/data-rel-schema (-> (for [^DataRel data-rel data-rels]
                              (-> (.getSchema data-rel)
                                  (.findField "op")
                                  (.getChildren) ^Field first))
                            (->> (apply types/merge-fields))
                            (types/field-with-name "put"))))

(defn exec-compaction-job! [^BufferAllocator allocator, ^BufferPool buffer-pool, ^PageMetadata$Factory metadata-mgr
                            {:keys [page-size]} {:keys [table-name part trie-keys out-trie-key]}]
  (try
    (log/debugf "compacting '%s' '%s' -> '%s'..." table-name trie-keys out-trie-key)

    (util/with-open [page-metadatas (LinkedList.)
                     data-rels (DataRel/openRels allocator buffer-pool table-name trie-keys)]
      (doseq [trie-key trie-keys]
        (.add page-metadatas (.openPageMetadata metadata-mgr (trie/->table-meta-file-path table-name trie-key))))

      (let [segments (mapv (fn [^PageMetadata page-meta data-rel]
                             (-> (trie/->Segment (.getTrie page-meta)) (assoc :data-rel data-rel)))
                           page-metadatas
                           data-rels)
            schema (->log-data-rel-schema (map :data-rel segments))

            data-file-size (util/with-open [data-rel (Relation. allocator schema)]
                             (merge-segments-into data-rel segments (byte-array part))

                             (util/with-open [trie-wtr (TrieWriter. allocator buffer-pool
                                                                    schema table-name out-trie-key
                                                                    true)]

                               (Compactor/writeRelation trie-wtr data-rel page-size)))]

        (log/debugf "compacted '%s' -> '%s'." table-name out-trie-key)

        [(cat/->added-trie table-name out-trie-key data-file-size)]))

    (catch ClosedByInterruptException _ (throw (InterruptedException.)))
    (catch InterruptedException e (throw e))

    (catch Throwable t
      (log/error t "Error running compaction job.")
      (throw t))))

(defrecord Job [table-name trie-keys part out-trie-key]
  Compactor$Job
  (getTableName [_] table-name)
  (getOutputTrieKey [_] out-trie-key))

(defn- l0->l1-compaction-job [table-name {l0 [0 nil []], l1c [1 nil []]} {:keys [^long file-size-target]}]
  (when-let [live-l0 (seq (->> l0
                               (take-while #(= :live (:state %)))))]

    (let [{l0-trie-key :trie-key, :keys [block-idx]} (last live-l0)
          {l1-trie-key :trie-key} (->> l1c
                                       (take-while #(< (:data-file-size %) file-size-target))
                                       first)]
      (->Job table-name (-> []
                            (cond-> l1-trie-key (conj l1-trie-key))
                            (conj l0-trie-key))
             nil ; recency
             (trie/->l1-trie-key nil block-idx)))))

(defn- l2h-input-files
  "if the tries can be compacted to L2H, return the input tries; otherwise nil"
  [live-tries {:keys [^long file-size-target]}]

  (->> live-tries

       (reductions (fn [[acc-tries acc-size] {:keys [^long data-file-size] :as trie}]
                     [(conj acc-tries trie) (+ acc-size data-file-size)])
                   [[] 0])

       (some (fn [[tries total-size]]
               (when (or (= cat/branch-factor (count tries))
                         (>= total-size file-size-target))
                 tries)))))

(defn- l2h-compaction-jobs [table-name table-tries opts]
  (for [[[level recency _part] tries] table-tries
        :when (and recency (= level 1))
        :let [input-tries (-> tries
                              (->> (take-while #(= :live (:state %))))
                              reverse
                              (l2h-input-files opts))]
        :when input-tries]
    (->Job table-name (mapv :trie-key input-tries) []
           (trie/->trie-key 2 recency nil (:block-idx (last input-tries))))))

(defn- tiering-compaction-jobs [table-name table-tries {:keys [^long file-size-target]}]
  (for [[[level recency part] files] table-tries
        :when (or (and (nil? recency) (= level 1))
                  (> level 1))
        :let [live-files (-> files
                             (->> (remove #(= :garbage (:state %))))
                             (cond->> (= level 1) (filter #(>= (:data-file-size %) file-size-target))))]
        :when (>= (count live-files) cat/branch-factor)
        :let [live-files (reverse live-files)]

        p (range cat/branch-factor)
        :let [out-level (inc level)
              out-part (conj part p)
              {lnp1-block-idx :block-idx} (first (get table-tries [out-level recency out-part]))

              in-files (-> live-files
                           (cond->> lnp1-block-idx (drop-while #(<= (:block-idx %) lnp1-block-idx)))
                           (->> (take cat/branch-factor)))
              out-part (byte-array out-part)]

        :when (= cat/branch-factor (count in-files))
        :let [trie-keys (mapv :trie-key in-files)
              out-block-idx (:block-idx (last in-files))]]
    (->Job table-name trie-keys out-part
           (trie/->trie-key out-level recency out-part out-block-idx))))

(defn compaction-jobs [table-name {table-tries :tries} opts]
  (concat (when-let [job (l0->l1-compaction-job table-name table-tries opts)]
            [job])
          (l2h-compaction-jobs table-name table-tries opts)
          (tiering-compaction-jobs table-name table-tries opts)))

(defmethod ig/prep-key :xtdb/compactor [_ ^CompactorConfig config]
  {:allocator (ig/ref :xtdb/allocator)
   :buffer-pool (ig/ref :xtdb/buffer-pool)
   :metadata-mgr (ig/ref :xtdb.metadata/metadata-manager)
   :threads (.getThreads config)
   :metrics-registry (ig/ref :xtdb.metrics/registry)
   :log (ig/ref :xtdb/log)
   :trie-catalog (ig/ref :xtdb/trie-catalog)})

(def ^:dynamic *page-size* 1024)

(defn- open-compactor [{:keys [allocator, ^BufferPool buffer-pool, ^Log log, ^TrieCatalog trie-catalog, metadata-mgr,
                               ^long threads metrics-registry]}]
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "compactor")]
    (metrics/add-allocator-gauge metrics-registry "compactor.allocator.allocated_memory" allocator)
    (let [page-size *page-size*]
      (Compactor/open
       (reify Compactor$Impl
         (availableJobs [_]
           (->> (.getTableNames trie-catalog)
                (into [] (mapcat (fn [table-name]
                                   (compaction-jobs table-name (cat/trie-state trie-catalog table-name) trie-catalog))))))

         (executeJob [_ job]
           (exec-compaction-job! allocator buffer-pool metadata-mgr {:page-size page-size} job))

         (close [_] (util/close allocator)))

       log trie-catalog *ignore-signal-block?* threads))))

(defmethod ig/init-key :xtdb/compactor [_ {:keys [threads] :as opts}]
  (if (pos? threads)
    (open-compactor opts)
    (Compactor/getNoop)))

(defmethod ig/halt-key! :xtdb/compactor [_ compactor]
  (util/close compactor))

(defn signal-block! [^Compactor compactor]
  (.signalBlock compactor))

(defn compact-all!
  ([node] (compact-all! node nil))
  ([node timeout] (.compactAll ^Compactor (util/component node :xtdb/compactor) timeout)))
