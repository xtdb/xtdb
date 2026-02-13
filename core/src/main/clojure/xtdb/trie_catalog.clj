(ns xtdb.trie-catalog
  (:require [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
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

(s/def ::level int?)
(s/def ::recency (s/nilable #(instance? LocalDate %)))
(s/def ::part (s/coll-of int? :kind vector?))

(s/def ::shard (s/tuple ::level ::recency ::part))

(s/def ::state #{:live :nascent :garbage})
(s/def ::block-idx int?)
(s/def ::trie (s/keys :req-un [::level ::recency ::part ::block-idx ::state]))

(defn descending-by? [key-fn]
  (fn [coll]
    (->> coll
         (map key-fn)
         (partition 2 1)
         (every? (fn [[a b]] (>= a b))))))

(s/def ::shard-list
  (s/and (s/coll-of ::trie)
         (descending-by? :block-idx)))

(defn all-eq? [key-fn value]
  (fn [coll]
    (every? #(= (key-fn %) value) coll)))

(s/def ::live (s/and ::shard-list
                     (all-eq? :state :live)))

(s/def ::nascent (s/and ::shard-list
                        (all-eq? :state :nascent)))

(s/def ::garbage (s/and ::shard-list
                        (all-eq? :state :garbage)))

(s/def ::max-block-idx ::block-idx)

(s/def ::shard-values
  (s/keys :req-un [::live ::garbage ::max-block-idx]
          :opt-un [::nascent]))

(s/def ::catalog-tries
  (s/nilable (s/map-of ::shard ::shard-values)))

;; table-tries data structure
;; values is a map of live, nascent and garbage tries lists
;; as well as the max-block-idx
;; tries :: {:keys [level recency part block-idx state]}
;;   sorted by block-idx desc
;;   this is true of the live, nascent and garbage tries
;;   for correctness it's important that these invariants are maintained when a trie switches state
;; part :: [long]
;; recency :: LocalDate
'{ ;; L0 files
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

(def ^:private empty-trie-state-list (sorted-set-by #(compare (:block-idx %2) (:block-idx %1))))

(defn- empty-shared
  ([] (empty-shared -1))
  ([max-block-idx] {:live empty-trie-state-list,
                    :garbage empty-trie-state-list,
                    :max-block-idx max-block-idx}))

(defn- supersede-partial-tries [tries
                                {:keys [^long block-idx] :as _trie}
                                {:keys [^long file-size-target as-of]}]
  ;; if there is no partition yet we intialize empty but set the max-block-idx the level above
  (let [{:keys [live garbage] :as tries} (or tries (empty-shared block-idx))
        new-garbage (->> live
                         (map (fn [{^long other-block-idx :block-idx, ^long other-size :data-file-size :as other-trie}]
                                (cond-> other-trie
                                  (and (< other-size file-size-target)
                                       (<= other-block-idx block-idx))
                                  (-> (assoc :state :garbage
                                             :garbage-as-of as-of)
                                      (dissoc :trie-metadata)))))
                         (filter (comp #{:garbage} :state)))]
    (-> tries
        (assoc :live (reduce disj live new-garbage))
        (assoc :garbage (into garbage new-garbage)))))


(defn- conj-trie [tries {block-idx :block-idx :as trie} state]
  (let [trie (assoc trie :state state)
        tries (or tries (empty-shared))]
    (when (and (not (nil? tries))
               (nil? (:live tries)))
      (throw (ex-info "tries missing live set" {:tries tries})))
    (-> tries
        (update state (fnil conj empty-trie-state-list) trie)
        (update :max-block-idx (fnil max -1) block-idx))))

(defn- insert-levelled-trie [tries trie opts]
  (-> tries
      (supersede-partial-tries trie opts)
      (conj-trie trie :live)))

(defn- supersede-by-block-idx [tries, ^long block-idx {:keys [as-of]}]
  ;; if there is no partition yet we intialize empty but set the max-block-idx from the level above
  (let [{:keys [live garbage] :as tries} (or tries (empty-shared block-idx))
        new-garbage (->> live
                         (map (fn [{^long other-block-idx :block-idx, :as trie}]
                                (cond-> trie
                                  (<= other-block-idx block-idx) (-> (assoc :state :garbage
                                                                            :garbage-as-of as-of)
                                                                     (dissoc :trie-metadata)))))
                         (filter (comp #{:garbage} :state)))]
    (-> tries
        (assoc :live (reduce disj live new-garbage))
        (assoc :garbage (into garbage new-garbage)))))

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
        ;; this should work as only the :block-idx is considered for comparison
        (update :nascent (fnil disj empty-trie-state-list) {:block-idx block-idx}))
    tries))

(defn- mark-part-group-live [table-tries {:keys [block-idx level recency part]}]
  (->> (let [pop-part (pop part)]
         (for [p (range branch-factor)]
           [level recency (conj pop-part p)]))

       (reduce (fn [table-tries shard-key]
                 (-> table-tries
                     (update shard-key mark-block-idx-live block-idx)))
               table-tries)))

(defn- insert-trie [table-cat {:keys [^long level, recency, part, ^long block-idx] :as trie} opts]
  (case (long level)
    0 (-> table-cat
          (update-in [:tries [0 recency part]] conj-trie trie :live))

    1 (if recency
        ;; L1H files are nascent until we see the corresponding L1C file
        (-> table-cat
            (update-in [:tries [1 recency part]] conj-trie trie
                       (let [{^long l1c-block-idx :block-idx, :as l1c} (first (get-in table-cat [:tries [1 nil part] :live]))]
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
                          (update [1 nil []] insert-levelled-trie trie opts)

                          ;; and supersede L0 files
                          (update [0 nil []] supersede-by-block-idx block-idx opts))))

            (update :l1h-recencies dissoc block-idx)))

    (if (and (= level 2) recency)
      ;; L2H
      (-> table-cat
          (update :tries
                  (fn [tries]
                    (-> tries
                        ;; L2H files are levelled, so this supersedes any previous partial files
                        (update [2 recency part] insert-levelled-trie trie opts)

                        ;; we supersede any L1H files that we've incorporated into this L2H
                        (update [1 recency []] supersede-by-block-idx block-idx opts)))))

      (-> table-cat
          (update-in [:tries [level recency part]] conj-trie trie :nascent)

          (update :tries
                  (fn [table-tries]
                    (cond-> table-tries
                      (completed-part-group? table-tries trie)
                      (-> (mark-part-group-live trie)
                          (update [(dec level) recency (cond-> part (seq part) pop)] supersede-by-block-idx block-idx opts)))))))))

(defn apply-trie-notification [table-cat trie opts]
  (cond-> table-cat
    (not (stale-msg? table-cat trie)) (insert-trie trie opts)))

(defn current-tries [{:keys [tries]}]
  (->> tries
       (into [] (mapcat (comp :live val)))))

(defn live-and-nascent-tries [{:keys [tries]}]
  (->> tries
       (into [] (mapcat (comp (fn [{:keys [live nascent]}] (concat live nascent)) val)))))

(defn all-tries [{:keys [tries]}]
  (->> (into [] (mapcat (comp (fn [{:keys [live nascent garbage]}] (concat live nascent garbage)) val)) tries)
       ;; the sort is needed as the table blocks need the current tries to be in the total order for restart
       (sort-by (juxt :level :block-idx #(or (:recency %) LocalDate/MAX)))))

(defn l0-tries
  [{:keys [tries]}]
  (let [{:keys [live garbage]} (get tries [0 nil []])]
    (concat live garbage)))

(defn l0-blocks
  "Returns a sorted seq of L0 blocks: [{:block-idx N, :tables [{:table TableRef, :trie-key String}]}]"
  [^xtdb.trie.TrieCatalog trie-cat]
  (->> (.getTables trie-cat)
       (mapcat (fn [table]
                 (map (fn [{:keys [block-idx trie-key]}]
                        [block-idx {:table table :trie-key trie-key}])
                      (l0-tries (trie-state trie-cat table)))))
       (group-by first)
       (map (fn [[block-idx pairs]]
              {:block-idx block-idx
               :tables (mapv second pairs)}))
       (sort-by :block-idx)))

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
  (let [garbage-by-path (->> (map trie/parse-trie-key garbage-trie-keys)
                             (group-by (juxt :level :recency :part)))]

    (update table-cat :tries
            (fn [trie-levels]
              (reduce (fn [trie-levels [path garbage-tries]]
                        (update-in trie-levels [path :garbage] #(reduce disj % garbage-tries)))
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
      (trie/filter-pages {:query-bounds query-bounds})))

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

(defn reset->l0 [{:keys [tries]}]
  ;; combine live & garbage (there should be no nascent)
  (let [{:keys [live garbage]} (get tries [0 nil []])
        l0-tries (concat live garbage)
        live-tries (->> l0-tries
                        (map #(assoc % :state :live))
                        (into empty-trie-state-list))]

    {:tries {[0 nil []] {:max-block-idx (:block-idx (first live-tries))
                         :live live-tries}}
     :l1h-recencies {}}))

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
                     (-> {:garbage (into empty-trie-state-list garbage)
                          :live (into empty-trie-state-list live)
                          :nascent (into empty-trie-state-list nascent)}
                         (assoc :max-block-idx max-block-idx)))))

(defrecord TrieCatalog [^BufferPool buffer-pool, ^BlockCatalog block-cat
                        ^Map !table-cats, ^long file-size-target]
  xtdb.trie.TrieCatalog
  (addTries [_ table added-tries as-of]
    (.compute !table-cats table
              (fn [_table tries]
                (log/tracef "Adding tries to table '%s': %s" table (mapv #(.getTrieKey ^TrieDetails %) added-tries))
                (try
                  (let [{:keys [tries] :as new-trie-cat}  (reduce (fn [table-cat ^TrieDetails added-trie]
                                                                    (if-let [parsed-key (trie/parse-trie-key (.getTrieKey added-trie))]
                                                                      (apply-trie-notification table-cat
                                                                                               (-> parsed-key
                                                                                                   (assoc :data-file-size (.getDataFileSize added-trie)
                                                                                                          :trie-metadata (.getTrieMetadata added-trie)))
                                                                                               {:file-size-target file-size-target,  :as-of as-of})
                                                                      table-cat))
                                                                  (or tries {})
                                                                  added-tries)]
                    (s/assert ::catalog-tries tries)
                    new-trie-cat)
                  (catch InterruptedException e (throw e))
                  (catch Throwable e
                    (log/error e "Failed to add tries to table" table)
                    (throw e))))))

  (getTables [_] (set (keys !table-cats)))

  (garbageTries [_ table as-of]
    (->> (garbage-tries (.get !table-cats table) as-of)
         (into #{} (map :trie-key))))

  (listAllGarbageTrieKeys [this table]
    (->> (:tries (trie-state this table))
         (mapcat (comp :garbage val))
         (into #{} (map :trie-key))))

  (deleteTries [_ table garbage-trie-keys]
    (.compute !table-cats table
              (fn [_table tries]
                (let [{:keys [tries] :as res} (remove-garbage tries garbage-trie-keys)]
                  (s/assert ::catalog-tries tries)
                  res))))

  (listAllTrieKeys [this table]
    (mapv :trie-key (all-tries (trie-state this table))))

  (listLiveAndNascentTrieKeys [this table]
    (mapv :trie-key (live-and-nascent-tries (trie-state this table))))

  (getPartitions [this table]
    (->> (trie-state this table)
         partitions
         (mapv (fn [partition]
                 (table-cat/->partition
                   (update partition :tries
                           (partial mapv #(trie/->trie-details table %))))))))

  (refresh [_]
    ;; HACK this is duplicated in `load-tries`
    (doseq [[table {:keys [partitions]}] (table-cat/load-tables-to-metadata buffer-pool block-cat)
            :let [partitions (update-vals partitions #(update % :tries (partial map trie/<-trie-details)))
                  tries (into {} (map partition->entry) partitions)
                  tries (if (new-partition? (first partitions))
                          tries
                          (merge-with merge tries (partitions->max-block-idx-map partitions)))]]
      (s/assert ::catalog-tries tries)
      (.put !table-cats table {:tries tries})))

  PTrieCatalog
  (trie-state [_ table] (.get !table-cats table))

  (reset->l0! [this]
    (doseq [table (.getTables this)]
      (.compute !table-cats table
                (fn [_table table-cat]
                  (reset->l0 table-cat))))))

(defmethod ig/expand-key :xtdb/trie-catalog [k opts]
  {k (into {:buffer-pool (ig/ref :xtdb/buffer-pool)
            :block-cat (ig/ref :xtdb/block-catalog)
            :table-cat (ig/ref :xtdb/table-catalog)}
           opts)})

(defn load-tries ^java.util.Map [table->table-block file-size-target]
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
        (s/assert ::catalog-tries tries)
        (.put !table-cats table {:tries tries}))

      !table-cats)

    ;; TODO: This else statement is here to support block files that have not yet passed to the new extended TrieDetails format
    ;; see #4526
    (let [cat (TrieCatalog. nil nil (ConcurrentHashMap.) file-size-target)
          now (Instant/now)]
      (doseq [[table {:keys [partitions]}] table->table-block
              {:keys [tries]} partitions]
        ;; As all tries get added afresh, max-block-idx is up to date for all existing partitions
        (.addTries cat table tries now))
      (:!table-cats cat))))

(defmethod ig/init-key :xtdb/trie-catalog [_ {:keys [^BufferPool buffer-pool, ^BlockCatalog block-cat]}]
  (log/debug "starting trie catalog...")
  (let [table->table-block (table-cat/load-tables-to-metadata buffer-pool block-cat)
        cat (TrieCatalog. buffer-pool block-cat
                          (load-tries table->table-block *file-size-target*)
                          *file-size-target*)]
    (log/debug "trie catalog started")
    cat))
