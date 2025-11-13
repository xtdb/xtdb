(ns xtdb.compactor
  (:require [integrant.core :as ig]
            [xtdb.db-catalog :as db]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as cat]
            [xtdb.util :as util])
  (:import com.carrotsearch.hppc.ByteArrayList
           xtdb.api.CompactorConfig
           (xtdb.compactor Compactor Compactor$Driver Compactor$Impl Compactor$Job Compactor$JobCalculator)
           (xtdb.trie Trie$Key)))

(def ^:dynamic *ignore-signal-block?* false)
(def ^:dynamic *recency-partition* nil)

(defrecord Job [table trie-keys part out-trie-key partitioned-by-recency?]
  Compactor$Job
  (getTable [_] table)
  (getTrieKeys [_] trie-keys)
  (getPart [_] part)
  (getOutputTrieKey [_] out-trie-key)
  (getPartitionedByRecency [_] partitioned-by-recency?))

(defn- l0->l1-compaction-job [table {{l0 :live} [0 nil []], {l1c :live} [1 nil []]} {:keys [^long file-size-target]}]
  (when-let [live-l0 (seq l0)]

    (let [{l0-trie-key :trie-key, :keys [block-idx]} (last live-l0)
          {l1-trie-key :trie-key} (->> l1c
                                       (take-while #(< (:data-file-size %) file-size-target))
                                       first)]
      (->Job table (-> []
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
       reverse

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

(defn- l2h-compaction-jobs [table table-tries opts]
  (for [[[level recency _part] {l1h-tries :live}] table-tries
        :when (and recency (= level 1))
        :let [input-tries (l2h-input-files (get-in table-tries [[2 recency []] :live]) l1h-tries opts)]
        :when input-tries]
    (->Job table (mapv :trie-key input-tries) (byte-array 0)
           (Trie$Key. 2 recency nil (:block-idx (last input-tries)))
           false ; partitioned-by-recency?
           )))

(defn- tiering-compaction-jobs [table table-tries {:keys [^long file-size-target]}]
  (for [[[level recency part] {files :live}] table-tries
        :when (or (and (nil? recency) (= level 1))
                  (> level 1))
        :let [live-files (-> files
                             (cond->> (or (= level 1) (and (= level 2) recency)) (filter #(>= (:data-file-size %) file-size-target))))]
        :when (>= (count live-files) cat/branch-factor)
        :let [live-files (reverse live-files)]

        p (range cat/branch-factor)
        :let [out-level (inc level)
              out-part (conj part p)
              {level-above-live :live level-above-nascent :nascent} (get table-tries [out-level recency out-part])
              {lnp1-live-block-idx :block-idx} (first level-above-live)
              {lnp1-nascent-block-idx :block-idx} (first level-above-nascent)
              lnp1-block-idx (or lnp1-nascent-block-idx lnp1-live-block-idx)

              in-files (-> live-files
                           (cond->> lnp1-block-idx (drop-while #(<= (:block-idx %) lnp1-block-idx)))
                           (->> (take cat/branch-factor)))
              out-part (byte-array out-part)]

        :when (= cat/branch-factor (count in-files))
        :let [trie-keys (mapv :trie-key in-files)
              out-block-idx (:block-idx (last in-files))]]
    (->Job table trie-keys out-part
           (Trie$Key. out-level recency (ByteArrayList/from out-part) out-block-idx)
           false ; partitioned-by-recency?
           )))

(defn compaction-jobs [table {table-tries :tries} opts]
  (concat (when-let [job (l0->l1-compaction-job table table-tries opts)]
            [job])
          (l2h-compaction-jobs table table-tries opts)
          (tiering-compaction-jobs table table-tries opts)))

(defmethod ig/expand-key :xtdb/compactor [k ^CompactorConfig config]
  {k {:threads (.getThreads config)
      :metrics-registry (ig/ref :xtdb.metrics/registry)}})

(def ^:dynamic *page-size* 1024)

(defrecord JobCalculator []
  Compactor$JobCalculator
  (availableJobs [_ trie-catalog]
    (->> (.getTables trie-catalog)
         (into [] (mapcat (fn [table]
                            (compaction-jobs table (cat/trie-state trie-catalog table) trie-catalog)))))))

(defn- open-compactor [{:keys [metrics-registry threads]}]
  (Compactor$Impl. (Compactor$Driver/real metrics-registry *page-size* *recency-partition*)
                   metrics-registry
                   (->JobCalculator)
                   *ignore-signal-block?* threads))

(defmethod ig/init-key :xtdb/compactor [_ {:keys [threads] :as opts}]
  (if (pos? threads)
    (open-compactor opts)
    Compactor/NOOP))

(defmethod ig/halt-key! :xtdb/compactor [_ compactor]
  (util/close compactor))

(defmethod ig/expand-key ::for-db [k {:keys [base]}]
  {k {:base base
      :query-db (ig/ref :xtdb.db-catalog/for-query)}})

(defmethod ig/init-key ::for-db [_ {{:keys [^Compactor compactor]} :base, :keys [query-db]}]
  (.openForDatabase compactor query-db))

(defmethod ig/halt-key! ::for-db [_ compactor-for-db]
  (util/close compactor-for-db))

(defn compact-all!
  "`timeout` is now required, explicitly specify `nil` if you want to wait indefinitely."
  [node timeout]

  (-> (.getCompactor (db/primary-db node))
      (.compactAll timeout)))
