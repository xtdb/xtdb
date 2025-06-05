(ns xtdb.compactor
  (:require [integrant.core :as ig]
            xtdb.metadata
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as cat]
            [xtdb.util :as util])
  (:import com.carrotsearch.hppc.ByteArrayList
           xtdb.api.CompactorConfig
           (xtdb.compactor Compactor Compactor$Impl Compactor$Job Compactor$JobCalculator)
           (xtdb.trie Trie$Key TrieCatalog)))

(def ^:dynamic *ignore-signal-block?* false)
(def ^:dynamic *recency-partition* nil)

(defrecord Job [table-name trie-keys part out-trie-key partitioned-by-recency?]
  Compactor$Job
  (getTableName [_] table-name)
  (getTrieKeys [_] trie-keys)
  (getPart [_] part)
  (getOutputTrieKey [_] out-trie-key)
  (getPartitionedByRecency [_] partitioned-by-recency?))

(defn- l0->l1-compaction-job [table-name {{l0 :live+nascent} [0 nil []], {l1c :live+nascent} [1 nil []]} {:keys [^long file-size-target]}]
  (when-let [live-l0 (seq (->> l0
                               (take-while #(= :live (:state %)))))]

    (let [{l0-trie-key :trie-key, :keys [block-idx]} (last live-l0)
          {l1-trie-key :trie-key} (->> l1c
                                       (take-while #(< (:data-file-size %) file-size-target))
                                       first)]
      (->Job table-name (-> []
                            (cond-> l1-trie-key (conj l1-trie-key))
                            (conj l0-trie-key))
             nil ; part
             (Trie$Key. 1 nil nil block-idx)
             true ; partitioned-by-recency?
             ))))

(defn- l2h-input-files
  "if the tries can be compacted to L2H, return the input tries; otherwise nil"
  [l2h-tries l1h-tries {:keys [^long file-size-target]}]

  (->> l1h-tries

       (reductions (fn [[acc-tries acc-size] {:keys [^long data-file-size] :as trie}]
                     [(conj acc-tries trie) (+ acc-size data-file-size)])
                   (or (when-let [{:keys [data-file-size] :as l2h-trie} (first l2h-tries)]
                         (when (< data-file-size file-size-target)
                           [[l2h-trie] data-file-size]))
                       [[] 0]))

       (some (fn [[tries total-size]]
               (when (or (= cat/branch-factor (count tries))
                         (>= total-size file-size-target))
                 tries)))))

(defn- l2h-compaction-jobs [table-name table-tries opts]
  (for [[[level recency _part] {l1h-tries :live+nascent}] table-tries
        :when (and recency (= level 1))
        :let [input-tries (l2h-input-files (-> (get-in table-tries [[2 recency []] :live+nascent])
                                               (->> (take-while #(= :live (:state %))))
                                               reverse)
                                           (-> l1h-tries
                                               (->> (take-while #(= :live (:state %))))
                                               reverse)
                                           opts)]
        :when input-tries]
    (->Job table-name (mapv :trie-key input-tries) (byte-array 0)
           (Trie$Key. 2 recency nil (:block-idx (last input-tries)))
           false ; partitioned-by-recency?
           )))

(defn- tiering-compaction-jobs [table-name table-tries {:keys [^long file-size-target]}]
  (for [[[level recency part] {files :live+nascent}] table-tries
        :when (or (and (nil? recency) (= level 1))
                  (> level 1))
        :let [live-files (-> files
                             (->> (remove #(= :garbage (:state %))))
                             (cond->> (or (= level 1) (and (= level 2) recency)) (filter #(>= (:data-file-size %) file-size-target))))]
        :when (>= (count live-files) cat/branch-factor)
        :let [live-files (reverse live-files)]

        p (range cat/branch-factor)
        :let [out-level (inc level)
              out-part (conj part p)
              {lnp1-block-idx :block-idx} (first (get-in table-tries [[out-level recency out-part] :live+nascent]))

              in-files (-> live-files
                           (cond->> lnp1-block-idx (drop-while #(<= (:block-idx %) lnp1-block-idx)))
                           (->> (take cat/branch-factor)))
              out-part (byte-array out-part)]

        :when (= cat/branch-factor (count in-files))
        :let [trie-keys (mapv :trie-key in-files)
              out-block-idx (:block-idx (last in-files))]]
    (->Job table-name trie-keys out-part
           (Trie$Key. out-level recency (ByteArrayList/from out-part) out-block-idx)
           false ; partitioned-by-recency?
           )))

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

(defn- open-compactor [{:keys [allocator buffer-pool metadata-mgr
                               log, ^TrieCatalog trie-catalog, metrics-registry
                               threads]}]
  (Compactor$Impl. allocator buffer-pool metadata-mgr
                   log trie-catalog metrics-registry
                   (reify Compactor$JobCalculator
                     (availableJobs [_]
                       (->> (.getTableNames trie-catalog)
                            (into [] (mapcat (fn [table-name]
                                               (compaction-jobs table-name (cat/trie-state trie-catalog table-name) trie-catalog)))))))

                   *ignore-signal-block?* threads *page-size* *recency-partition*))

(defmethod ig/init-key :xtdb/compactor [_ {:keys [threads] :as opts}]
  (if (pos? threads)
    (open-compactor opts)
    Compactor/NOOP))

(defmethod ig/halt-key! :xtdb/compactor [_ compactor]
  (util/close compactor))

(defn signal-block! [^Compactor compactor]
  (.signalBlock compactor))

(defn compact-all!
  "`timeout` is now required, explicitly specify `nil` if you want to wait indefinitely."
  [node timeout]

  (.compactAll ^Compactor (util/component node :xtdb/compactor) timeout))
