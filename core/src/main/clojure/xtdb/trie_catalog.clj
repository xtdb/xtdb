(ns xtdb.trie-catalog
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.table-catalog :as table-cat]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import [clojure.lang MapEntry]
           [java.nio ByteBuffer]
           [java.time Instant LocalDate ZoneOffset]
           [java.util Map]
           [java.util.concurrent ConcurrentHashMap]
           org.roaringbitmap.buffer.ImmutableRoaringBitmap
           xtdb.catalog.BlockCatalog
           (xtdb.log.proto TemporalMetadata TrieDetails TrieMetadata)
           (xtdb.segment Segment$PageMeta)
           (xtdb.storage BufferPool)
           (xtdb.util TemporalBounds)))

;; table-tries data structure
;; values is a map of live, nascent and garbage tries lists
;; as well as the max-block-idx
;; tries :: {:keys [level recency part block-idx state]}
;;   sorted by block-idx desc
;;   this is true of the live, nascent and garbage tries
;;   for correctness it's important that these invariants are maintained when a trie switches state
;; part :: [long]
;; recency :: LocalDate
'{;; L0 files
  [0 nil []] {:live () :nascent () :garbage ()}

  ;; L1 current files (L1C)
  [1 nil []] {:live () :nascent () :garbage ()}

  ;; L2C - no recency but we do have a (single-element) part
  [2 nil part] {:live () :nascent () :garbage ()}

  ;; L2H - no part yet but we do now have recency
  [2 recency []] {:live () :nascent () :garbage ()}

  ;; L3+ have both recency (if applicable) and part
  ;; if they have a recency they'll have one fewer part element
  [3 recency part] {:live () :nascent () :garbage ()}}

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
  (trie-state [trie-cat table])

  (reset->l0! [trie-cat]
    "DANGER: resets the trie-catalog back to L0,
     use if compaction has gone awry, and you want to completely recompact.")
  (reset->l2h! [trie-cat]
    "DANGER: resets the trie-catalog back to L2h,
     use if compaction has gone awry, and you want to completely recompact."))

(def ^:const branch-factor 4)

(def ^:dynamic ^{:tag 'long} *file-size-target* (* 100 1024 1024))

(defn- stale-block-idx? [{:keys [^long max-block-idx] :or {max-block-idx -1} :as _tries} ^long block-idx]
  (>= max-block-idx block-idx))

(defn stale-msg?
  "messages have a total ordering, within their level/recency/part - so we know if we've received a message out-of-order.
   we check the staleness of messages so that we don't later have to try to insert a message within a list - we only ever need to prepend it"
  [{table-tries :tries} {:keys [^long level, part, recency, ^long block-idx]}]

  (stale-block-idx? (get table-tries [level recency part]) block-idx))

(defn- filter-garbage [tries]
  ((juxt filter remove) #(= (:state %) :garbage) tries))

(defn- supersede-partial-tries [{:keys [live garbage] :as tries}
                                {:keys [^long block-idx] :as _trie}
                                {:keys [^long file-size-target]}
                                as-of]
  (let [[new-garbage live] (->> live
                                ;; TODO this simple map op does not work, see #4947
                                (map (fn [{^long other-block-idx :block-idx, ^long other-size :data-file-size :as other-trie}]
                                       (cond-> other-trie
                                         (and (< other-size file-size-target)
                                              (<= other-block-idx block-idx))
                                         (-> (assoc :state :garbage
                                                    :garbage-as-of as-of)
                                             (dissoc :trie-metadata)))))
                                filter-garbage)]
    (-> tries
        (assoc :live (doall live))
        (assoc :garbage (doall (concat new-garbage garbage))))))

(defn- conj-trie [tries {block-idx :block-idx :as trie} state]
  (let [trie (assoc trie :state state)
        tries (or tries {:live (), :nascent (), :garbage ()})]
    (-> tries
        (update state conj trie)
        (update :max-block-idx (fnil max -1) block-idx))))

(defn- insert-levelled-trie [tries trie trie-cat as-of]
  (-> tries
      (supersede-partial-tries trie trie-cat as-of)
      (conj-trie trie :live)))

(defn- supersede-by-block-idx [{:keys [live garbage] :as tries}, ^long block-idx as-of]
  (let [[new-garbage live] (->> live
                                (map (fn [{^long other-block-idx :block-idx, :as trie}]
                                       (cond-> trie
                                         (<= other-block-idx block-idx) (-> (assoc :state :garbage
                                                                                   :garbage-as-of as-of)
                                                                            (dissoc :trie-metadata)))))
                                filter-garbage)]
    (-> tries
        (assoc :live (doall live))
        (assoc :garbage (doall (concat new-garbage garbage))))))

(defn- sibling-tries [table-tries, {:keys [^long level, recency, part]}]
  (let [pop-part (pop part)]
    (for [p (range branch-factor)]
      (get table-tries [level recency (conj pop-part p)]))))

(defn- completed-part-group? [table-tries {:keys [^long block-idx] :as trie}]
  (->> (sibling-tries table-tries trie)
       (every? (fn [{:keys [^long max-block-idx] :or {max-block-idx -1} :as _ln}]
                 (>= max-block-idx block-idx)))))

(defn- mark-block-idx-live [{:keys [nascent] :as tries} ^long block-idx]
  (if-let [trie (first (filter #(= (:block-idx %) block-idx) nascent))]
    (-> tries
        (conj-trie trie :live)
        (update :nascent (fn [nascents] (doall (remove #(= (:block-idx %) block-idx) nascents)))))
    tries))

(defn- mark-part-group-live [table-tries {:keys [block-idx level recency part]}]
  (->> (let [pop-part (pop part)]
         (for [p (range branch-factor)]
           [level recency (conj pop-part p)]))

       (reduce (fn [table-tries shard-key]
                 (-> table-tries
                     (update shard-key mark-block-idx-live block-idx)))
               table-tries)))

(defn- insert-trie [table-cat {:keys [^long level, recency, part, ^long block-idx] :as trie} trie-cat as-of]
  (case (long level)
    0 (-> table-cat
          (update-in [:tries [0 recency part]] conj-trie trie :live))

    1 (if recency
        ;; L1H files are nascent until we see the corresponding L1C file
        (-> table-cat
            (update-in [:tries [1 recency part]] conj-trie trie
                       (let [[{^long l1c-block-idx :block-idx, :as l1c}] (get-in table-cat [:tries [1 nil part] :live])]
                         (if (and l1c (>= l1c-block-idx block-idx))
                           :live :nascent)))
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
                          (update [1 nil []] insert-levelled-trie trie trie-cat as-of)

                          ;; and supersede L0 files
                          (update [0 nil []] supersede-by-block-idx block-idx as-of))))

            (update :l1h-recencies dissoc block-idx)))

    (if (and (= level 2) recency)
      ;; L2H
      (-> table-cat
          (update :tries
                  (fn [tries]
                    (-> tries
                        ;; L2H files are levelled, so this supersedes any previous partial files
                        (update [2 recency part] insert-levelled-trie trie trie-cat as-of)

                        ;; we supersede any L1H files that we've incorporated into this L2H
                        (update [1 recency []] supersede-by-block-idx block-idx as-of)))))

      (-> table-cat
          (update-in [:tries [level recency part]] conj-trie trie :nascent)

          (update :tries
                  (fn [table-tries]
                    (cond-> table-tries
                      (completed-part-group? table-tries trie)
                      (-> (mark-part-group-live trie)
                          (update [(dec level) recency (cond-> part (seq part) pop)] supersede-by-block-idx block-idx as-of)))))))))

(defn apply-trie-notification
  ([trie-cat table-cat trie]
   (apply-trie-notification trie-cat table-cat trie nil))
  ([trie-cat table-cat trie as-of]
   (let [trie (-> trie (update :part vec))]
     (cond-> table-cat
       (not (stale-msg? table-cat trie)) (insert-trie trie trie-cat as-of)))))

(defn current-tries [{:keys [tries]}]
  (->> tries
       (into [] (mapcat (comp :live val)))))

(defn all-tries [{:keys [tries]}]
  (->> (into [] (mapcat (comp (fn [{:keys [live nascent garbage]}] (concat live nascent garbage)) val)) tries)
       ;; the sort is needed as the table blocks need the current tries to be in the total order for restart
       (sort-by (juxt :level :block-idx #(or (:recency %) LocalDate/MAX)))))

(defn partitions [{:keys [tries]}]
  (map (fn [[[level recency part] {:keys [max-block-idx live nascent garbage]}]]
         {:level level
          :recency recency
          :part part
          :max-block-idx max-block-idx
          :tries (concat live nascent garbage)})
       tries))

(defn garbage-fn [as-of]
  (fn [{:keys [level garbage-as-of]}]
    (when (not= level 0)
      (<= (compare garbage-as-of as-of) 0))))

(defn garbage-tries [{:keys [tries]} as-of]
  (->> (mapcat (comp :garbage val) tries)
       (filter (garbage-fn as-of))))

(defn remove-garbage [table-cat garbage-trie-keys]
  (let [garbage-by-path (-> (->> (map trie/parse-trie-key garbage-trie-keys)
                                 (group-by (juxt :level :recency :part)))
                            (update-vals #(map :trie-key %)))]

    (update table-cat :tries
            (fn [trie-levels]
              (reduce (fn [trie-levels [path garbage-trie-keys]]
                        (update-in trie-levels [path :garbage] #(remove (comp (set garbage-trie-keys) :trie-key) %)))
                      trie-levels
                      garbage-by-path)))))

(def ^:private no-temporal-metadata
  (-> (TemporalMetadata/newBuilder)
      (.setMinValidFrom Long/MIN_VALUE)
      (.setMaxValidFrom Long/MAX_VALUE)
      (.setMinValidTo Long/MIN_VALUE)
      (.setMaxValidTo Long/MAX_VALUE)
      (.setMinSystemFrom Long/MIN_VALUE)
      (.setMaxSystemFrom Long/MAX_VALUE)
      (.build)))

(defrecord CatalogEntry [^LocalDate recency ^TrieMetadata trie-metadata ^TemporalBounds query-bounds]
  Segment$PageMeta
  (testMetadata [_]
    (let [min-query-recency (min (.getLower (.getValidTime query-bounds)) (.getLower (.getSystemTime query-bounds)))]
      (if-let [^long recency (and recency (time/instant->micros (time/->instant recency {:default-tz ZoneOffset/UTC})))]
        ;; the recency of a trie is exclusive, no row in that file has a recency equal to it
        (< min-query-recency recency)
        true)))

  (getRecency [_]
    (if recency
      (time/instant->micros (time/->instant recency {:default-tz ZoneOffset/UTC}))
      Long/MAX_VALUE))

  (getTemporalMetadata [_]
    ;; if we reset compaction, we may not have temporal metadata until the next compaction
    ;; so we need to treat the file as if it contains _anything_
    (or (some-> trie-metadata .getTemporalMetadata)
        no-temporal-metadata)))

(defn filter-tries [tries query-bounds]
  (-> (map (comp map->CatalogEntry #(assoc % :query-bounds query-bounds)) tries)
      (trie/filter-pages query-bounds)))

(defn <-trie-metadata [^TrieMetadata trie-metadata]
  (when (and trie-metadata (.hasTemporalMetadata trie-metadata))
    (let [^TemporalMetadata temporal-metadata (.getTemporalMetadata trie-metadata)]
      {:min-valid-from (time/micros->instant  (.getMinValidFrom temporal-metadata))
       :max-valid-from (time/micros->instant (.getMaxValidFrom temporal-metadata))
       :min-valid-to (time/micros->instant (.getMinValidTo temporal-metadata))
       :max-valid-to (time/micros->instant (.getMaxValidTo temporal-metadata))
       :min-system-from (time/micros->instant (.getMinSystemFrom temporal-metadata))
       :max-system-from (time/micros->instant (.getMaxSystemFrom temporal-metadata))
       :row-count (.getRowCount trie-metadata)
       :iid-bloom (ImmutableRoaringBitmap. (ByteBuffer/wrap (.toByteArray (.getIidBloom trie-metadata))))})))

(defn compacted-trie-keys [{:keys [tries]}]
  (for [[k tries] tries
        :when (not= k [0 nil []])
        :let [{:keys [live nascent garbage]} tries
              tries (concat live nascent garbage)]
        {:keys [trie-key]} tries]
    trie-key))

(defn compacted-trie-keys-syn-l3h [{:keys [tries]}]
  (for [[k tries] tries
        :let [[level recency _part] k]
        :when (and (> level 2) recency
                   (neg? (compare recency #xt/date "2025-12-01")))
        :let [{:keys [live nascent garbage]} tries
              tries (concat live nascent garbage)]
        {:keys [trie-key]} tries]
    trie-key))

(defn reset->l0 [{:keys [tries]}]
  ;; combine live & garbage (there should be no nascent)
  (let [{:keys [live garbage]} (get tries [0 nil []])
        l0-tries (concat live garbage)
        live-tries (->> l0-tries
                        (sort-by :block-idx #(compare %2 %1))
                        (map #(assoc % :state :live)))]

    {:tries {[0 nil []] {:max-block-idx (:block-idx (first live-tries))
                         :live (doall live-tries)}}
     :l1h-recencies {}}))

;; - remove L3H and above within this recency range
;; - preserve :l1h-recencies rather than clearing it
;; - L2H - N partitions. preserve outside this recency range, update within
;; - set in-range L2H files to live where data-file-size > file-size-target

(defn reset->l2h [{:keys [tries l1h-recencies]} file-size-target]
  {:tries
   (->> tries
        (into {} (keep (fn [[[level recency part :as k] tries]]
                         (cond (and (> level 2)
                                    recency
                                    (neg? (compare recency #xt/date "2025-12-01")))
                               nil
                               (and (= level 2)
                                    recency
                                    (neg? (compare recency #xt/date "2025-12-01")))
                               [k
                                (let [{:keys [live garbage]} tries
                                      {full true partial false} (group-by #(>= (:data-file-size %) file-size-target) garbage)]
                                  {:live (concat live (map #(-> %
                                                                (assoc :state :live)
                                                                (dissoc :garbage-as-of)) full))
                                   :garbage partial})]
                               
                               :else
                               [k tries])))))
   :l1h-recencies l1h-recencies})

(defrecord TrieCatalog [^Map !table-cats, ^long file-size-target]
  xtdb.trie.TrieCatalog
  (addTries [this table added-tries as-of]
    (.compute !table-cats table
              (fn [_table tries]
                (log/tracef "Adding tries to table '%s': %s" table (mapv #(.getTrieKey ^TrieDetails %) added-tries))
                (try
                  (reduce (fn [table-cat ^TrieDetails added-trie]
                            (if-let [parsed-key (trie/parse-trie-key (.getTrieKey added-trie))]
                              (apply-trie-notification this table-cat
                                                       (-> parsed-key
                                                           (assoc :data-file-size (.getDataFileSize added-trie)
                                                                  :trie-metadata (.getTrieMetadata added-trie)))
                                                       as-of)
                              table-cat))
                          (or tries {})
                          added-tries)
                  (catch InterruptedException e (throw e))
                  (catch Throwable e
                    (log/error e "Failed to add tries to table" table)
                    (throw e))))))

  (getTables [_] (set (keys !table-cats)))

  (garbageTries [_ table as-of]
    (->> (garbage-tries (.get !table-cats table) as-of)
         (into #{} (map :trie-key))))

  (deleteTries [_ table garbage-trie-keys]
    (.compute !table-cats table
              (fn [_table tries]
                (remove-garbage tries garbage-trie-keys))))

  (listAllTrieKeys [this table]
    (mapv :trie-key (all-tries (trie-state this table))))

  PTrieCatalog
  (trie-state [_ table] (.get !table-cats table))

  (reset->l0! [this]
    (doseq [table (.getTables this)]
      (.compute !table-cats table
                (fn [_table table-cat]
                  (reset->l0 table-cat)))))
  (reset->l2h! [this]
    (doseq [table (.getTables this)]
      (.compute !table-cats table
                (fn [_table table-cat]
                  (reset->l2h table-cat file-size-target))))))

(defmethod ig/expand-key :xtdb/trie-catalog [k opts]
  {k (into {:buffer-pool (ig/ref :xtdb/buffer-pool)
            :block-cat (ig/ref :xtdb/block-catalog)
            :table-cat (ig/ref :xtdb/table-catalog)}
           opts)})

(defn new-trie-details? [^TrieDetails trie-details]
  (.hasTrieState trie-details))

(defn partitions->max-block-idx-map [partitions]
  (let [ps->max-block-idx (into {} (map (fn [{:keys [level recency part tries]}]
                                          [[level recency part]
                                           (or (some->> tries (map :block-idx) seq (apply max)) 0)])) partitions)]
    (loop [res ps->max-block-idx [[[level recency part] max-block-idx] & rest] (seq ps->max-block-idx)]
      (if-not level
        (update-vals res #(hash-map :max-block-idx %))
        (recur
         (loop [res res level (dec ^long level) part (cond-> part (seq part) pop)]
           (if (< level 1)
             res
             (recur (update res [level recency part] (fnil max 0) max-block-idx)
                    (dec level)
                    (cond-> part (seq part) pop))))
         rest)))))

(defn new-partition? [partition]
  (some? (:max-block-idx partition)))

(defn partition->entry [{:keys [level recency part tries max-block-idx] :as _partition}]
  (MapEntry/create [level recency part]
                   (let [{:keys [live nascent garbage]} (group-by :state tries)]
                     (-> {:garbage garbage :live live :nascent nascent}
                         (update-vals (fn [trie-list] (sort-by :block-idx #(compare %2 %1) trie-list)))
                         (assoc :max-block-idx max-block-idx)))))

(defn trie-catalog-init [table->table-block]
  ;; We need to check one trie-details message from every table as table block files might come from different nodes. #4664
  ;; Partitions can be empty, so we need to get the first non-empty. #5017
  (if (->> (vals table->table-block)
           (map (comp first :tries first (partial drop-while (comp empty? :tries)) :partitions))
           (filter some?) ;; all empty partitions, should not happen in practice
           (every? new-trie-details?))
    (let [!table-cats (ConcurrentHashMap.)]
      (doseq [[table {:keys [partitions]}] table->table-block
              :let [partitions (update-vals partitions #(update % :tries (partial map trie/<-trie-details)))
                    tries (into {} (map partition->entry) partitions)
                    tries (if (new-partition? (first partitions))
                            tries
                            (merge-with merge tries (partitions->max-block-idx-map partitions)))]]
        (.put !table-cats table {:tries tries}))

      (TrieCatalog. !table-cats *file-size-target*))

    ;; TODO: This else statement is here to support block files that have not yet passed to the new extended TrieDetails format
    ;; see #4526
    (let [cat (TrieCatalog. (ConcurrentHashMap.) *file-size-target*)
          now (Instant/now)]
      (doseq [[table {:keys [partitions]}] table->table-block
              {:keys [tries]} partitions]
        ;; As all tries get added afresh, max-block-idx is up to date for all existing partitions
        (.addTries cat table tries now))
      cat)))

(defmethod ig/init-key :xtdb/trie-catalog [_ {:keys [^BufferPool buffer-pool, ^BlockCatalog block-cat]}]
  (log/debug "starting trie catalog...")
  (let [[_ table->table-block] (table-cat/load-tables-to-metadata buffer-pool block-cat)
        cat (trie-catalog-init table->table-block)]
    (log/debug "trie catalog started")
    cat))
