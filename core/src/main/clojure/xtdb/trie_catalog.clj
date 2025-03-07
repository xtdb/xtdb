(ns xtdb.trie-catalog
  (:require [integrant.core :as ig]
            [xtdb.trie :as trie]
            [xtdb.util :as util]
            [xtdb.time :as time]
            [xtdb.table-catalog :as table-cat])
  (:import org.roaringbitmap.buffer.ImmutableRoaringBitmap
           [java.nio ByteBuffer]
           [java.util Map]
           [java.util.concurrent ConcurrentHashMap]
           (xtdb BufferPool)
           xtdb.catalog.BlockCatalog
           (xtdb.log.proto TrieDetails TrieMetadata)))

;; table-tries data structure
;; values :: {:keys [level recency part block-idx state]}
;;   sorted by block-idx desc (except L1H)
;; part :: [long]
;; recency :: LocalDate
'{;; L0 files
  [0 nil []] ()

  ;; L1 current files (L1C)
  [1 nil []] ()

  ;; L2C - no recency but we do have a (single-element) part
  [2 nil part] ()

  ;; L2H - no part yet but we do now have recency
  [2 recency []] ()

  ;; L3+ have both recency (if applicable) and part
  ;; if they have a recency they'll have one fewer part element
  [3 recency part] ()}

;; The journey of a row:
;; 1. written to L0 by the indexer
;; 2. L0 then compacted to L1, split out into current (L1C) and historical (L1H)
;;    - L1H files are *tiered* (i.e. not written again by L1 compaction)
;; 3. L1C files are *levelled* - while we have an incomplete (< `*file-size-target*`) L1C file,
;;    we then compact more L0 files into it, each time creating another L1C file (which supersedes the first) and more L1H files (which don't, because they're tiered).
;; 4a. When we have `branch-factor` L1C files, we then compact them into an L2C file, and so on deeper into the current tree.
;;     L2+C files are *tiered*.
;; 4b. L1H files are compacted into L2H files.
;;     L2H files are *levelled* - i.e. we keep incorporating L1H files in until we have a full L2H file.
;;     We then compact `branch-factor` L2H files into an L3H file, sharding by IID, and so on deeper into the historical tree.
;;     L3+H files are *tiered*.

;; The impact of this is that historical files are 'one level behind' current files, in terms of IID sharding
;; e.g. L4C files are sharded by three IID parts; L4H are sharded by recency and two IID parts.

(set! *unchecked-math* :warn-on-boxed)

(defprotocol PTrieCatalog
  (trie-state [trie-cat table-name]))

(def ^:const branch-factor 4)

(def ^:dynamic ^{:tag 'long} *file-size-target* (* 100 1024 1024))

(defn- map-while
  "map f until it returns nil, then append the rest of the collection unaltered."
  [f coll]

  (lazy-seq
   (when-let [[x & xs] (seq coll)]
     (let [v (f x)]
       (if v
         (cons v (map-while f xs))
         (cons x xs))))))

(defn- stale-block-idx? [tries ^long block-idx]
  (when-let [{^long other-block-idx :block-idx} (first tries)]
    (>= other-block-idx block-idx)))

(defn stale-msg?
  "messages have a total ordering, within their level/recency/part - so we know if we've received a message out-of-order.
   we check the staleness of messages so that we don't later have to try to insert a message within a list - we only ever need to prepend it"
  [{table-tries :tries} {:keys [^long level, part, recency, ^long block-idx]}]

  (stale-block-idx? (get table-tries [level recency part]) block-idx))

(defn- supersede-partial-tries [tries {:keys [^long block-idx]} {:keys [^long file-size-target]}]
  (->> tries
       (map-while (fn [{^long other-block-idx :block-idx, ^long other-size :data-file-size, other-state :state, :as other-trie}]
                    (when-not (= other-state :garbage)
                      (cond-> other-trie
                        (and (= other-state :live)
                             (< other-size file-size-target)
                             (<= other-block-idx block-idx))
                        (assoc :state :garbage)))))))

(defn- conj-trie [tries trie state]
  (conj (or tries '()) (assoc trie :state state)))

(defn- insert-levelled-trie [tries trie trie-cat]
  (-> tries
      (supersede-partial-tries trie trie-cat)
      (conj-trie trie :live)))

(defn- supersede-by-block-idx [tries, ^long block-idx]
  (->> tries
       (map-while (fn [{other-state :state, ^long other-block-idx :block-idx, :as trie}]
                    (when-not (= other-state :garbage)
                      (cond-> trie
                        (<= other-block-idx block-idx) (assoc :state :garbage)))))))

(defn- sibling-tries [table-tries, {:keys [^long level, recency, part]}]
  (let [pop-part (pop part)]
    (for [p (range branch-factor)]
      (get table-tries [level recency (conj pop-part p)]))))

(defn- completed-part-group? [table-tries {:keys [^long block-idx] :as trie}]
  (->> (sibling-tries table-tries trie)
       (every? (fn [ln]
                 (when-let [{^long ln-block-idx :block-idx, ln-state :state} (first ln)]
                   (or (> ln-block-idx block-idx)
                       (= :nascent ln-state)))))))

(defn- mark-block-idx-live [tries ^long block-idx]
  (->> tries
       (map-while (fn [{trie-state :state, trie-block-idx :block-idx, :as trie}]
                    (cond-> trie
                      (and (= trie-state :nascent)
                           (= trie-block-idx block-idx))
                      (assoc :state :live))))))

(defn- mark-part-group-live [table-tries {:keys [block-idx level recency part]}]
  (->> (let [pop-part (pop part)]
         (for [p (range branch-factor)]
           [level recency (conj pop-part p)]))

       (reduce (fn [table-tries shard-key]
                 (-> table-tries
                     (update shard-key mark-block-idx-live block-idx)))
               table-tries)))

(defn- insert-trie [table-cat {:keys [^long level, recency, part, ^long block-idx] :as trie} trie-cat]
  (case (long level)
    0 (-> table-cat
          (update-in [:tries [0 recency part]] conj-trie trie :live))

    1 (if recency
        ;; L1H files are nascent until we see the corresponding L1C file
        (-> table-cat
            (update-in [:tries [1 recency part]] conj-trie trie :nascent)
            (update-in [:l1h-recencies block-idx] (fnil conj #{}) recency))

        ;; L1C
        (-> table-cat

            (update :tries
                    (fn [table-tries]
                      (-> table-tries

                          ;; mark L1H files live
                          (as-> table-tries (reduce (fn [acc recency]
                                                      (-> acc (update [1 recency []] mark-block-idx-live block-idx)))
                                                    table-tries
                                                    (get-in table-cat [:l1h-recencies block-idx])))

                          ;; L1C files are levelled, so this supersedes any previous partial files
                          (update [1 nil []] insert-levelled-trie trie trie-cat)

                          ;; and supersede L0 files
                          (update [0 nil []] supersede-by-block-idx block-idx))))

            (update :l1h-recencies dissoc block-idx)))

    (if (and (= level 2) recency)
      ;; L2H
      (-> table-cat
          (update :tries
                  (fn [tries]
                    (-> tries
                        ;; L2H files are levelled, so this supersedes any previous partial files
                        (update [2 recency part] insert-levelled-trie trie trie-cat)

                        ;; we supersede any L1H files that we've incorporated into this L2H
                        (update [1 recency []] supersede-by-block-idx block-idx)))))

      (-> table-cat
          (update-in [:tries [level recency part]] conj-trie trie :nascent)

          (update :tries
                  (fn [table-tries]
                    (cond-> table-tries
                      (completed-part-group? table-tries trie)
                      (-> (mark-part-group-live trie)
                          (update [(dec level) recency (when (seq part) (pop part))] supersede-by-block-idx block-idx)))))))))

(defn apply-trie-notification [trie-cat table-cat trie]
  (let [trie (-> trie (update :part vec))]
    (cond-> table-cat
      (not (stale-msg? table-cat trie)) (insert-trie trie trie-cat))))

(defn current-tries [{:keys [tries]}]
  (->> tries
       (into [] (comp (mapcat val)
                      (filter #(= (:state %) :live))))
       (sort-by :block-idx)))

(defn all-tries [{:keys [tries]}]
  (->> (into [] (mapcat val) tries)
       (sort-by :block-idx)))

(defn <-trie-metadata [^TrieMetadata trie-metadata]
  (when (.hasMinValidFrom trie-metadata)
    {:min-valid-from (time/micros->instant  (.getMinValidFrom trie-metadata))
     :max-valid-from (time/micros->instant (.getMaxValidFrom trie-metadata))
     :min-valid-to (time/micros->instant (.getMinValidTo trie-metadata))
     :max-valid-to (time/micros->instant (.getMaxValidTo trie-metadata))
     :min-system-from (time/micros->instant (.getMinSystemFrom trie-metadata))
     :max-system-from (time/micros->instant (.getMaxSystemFrom trie-metadata))
     :row-count (.getRowCount trie-metadata)
     :iid-bloom (ImmutableRoaringBitmap. (ByteBuffer/wrap (.toByteArray (.getIidBloom trie-metadata))))}))

(defrecord TrieCatalog [^Map !table-cats, ^long file-size-target]
  xtdb.trie.TrieCatalog
  (addTries [this added-tries]
    (doseq [[table-name added-tries] (->> added-tries
                                          (group-by #(.getTableName ^TrieDetails %)))]
      (.compute !table-cats table-name
                (fn [_table-name tries]
                  (reduce (fn [table-cat ^TrieDetails added-trie]
                            (if-let [parsed-key (trie/parse-trie-key (.getTrieKey added-trie))]
                              (apply-trie-notification this table-cat
                                                       (-> parsed-key
                                                           (assoc :data-file-size (.getDataFileSize added-trie)
                                                                  :trie-metadata (.getTrieMetadata added-trie))))
                              table-cat))
                          (or tries {})
                          added-tries)))))

  (getTableNames [_] (set (keys !table-cats)))

  PTrieCatalog
  (trie-state [_ table-name] (.get !table-cats table-name)))

(defmethod ig/prep-key :xtdb/trie-catalog [_ opts]
  (into {:buffer-pool (ig/ref :xtdb/buffer-pool)
         :block-cat (ig/ref :xtdb/block-catalog)
         :table-cat (ig/ref :xtdb/table-catalog)}
        opts))

(defmethod ig/init-key :xtdb/trie-catalog [_ {:keys [^BufferPool buffer-pool, ^BlockCatalog block-cat]}]
  (let [[_ table->table-block] (table-cat/load-tables-to-metadata buffer-pool block-cat)]
    (doto (TrieCatalog. (ConcurrentHashMap.) *file-size-target*)
      (.addTries (for [[_ {:keys [current-tries]}] table->table-block
                       current-trie current-tries]
                   current-trie)))))

(defn trie-catalog ^xtdb.trie.TrieCatalog [node]
  (util/component node :xtdb/trie-catalog))
