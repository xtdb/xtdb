(ns xtdb.compactor.job-calculator
  (:require [xtdb.trie-catalog :as cat])
  (:import com.carrotsearch.hppc.ByteArrayList
           (xtdb.compactor Compactor$Job Compactor$JobCalculator)
           (xtdb.trie Trie$Key)))

;; see `dev/doc/compaction.md` for an explanation of the compaction strategy.

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
              {lnp1-block-idx :max-block-idx} (get table-tries [out-level recency out-part])

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

(defrecord JobCalculator []
  Compactor$JobCalculator
  (availableJobs [_ trie-catalog]
    (->> (.getTables trie-catalog)
         (into [] (mapcat (fn [table]
                            (compaction-jobs table (cat/trie-state trie-catalog table) trie-catalog)))))))
