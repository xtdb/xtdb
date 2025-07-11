(ns xtdb.trie-catalog
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.table-catalog :as table-cat]
            [xtdb.time :as time]
            [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import [java.nio ByteBuffer]
           [java.time LocalDate ZoneOffset Instant]
           [java.util Map]
           [java.util.concurrent ConcurrentHashMap]
           org.roaringbitmap.buffer.ImmutableRoaringBitmap
           (xtdb BufferPool)
           xtdb.catalog.BlockCatalog
           (xtdb.log.proto TemporalMetadata TrieDetails TrieMetadata)
           xtdb.operator.scan.Metadata
           (xtdb.trie Trie)
           (xtdb.util TemporalBounds)))

;; table-tries data structure
;; values is a map of live+nascent and garbage tries
;; tries :: {:keys [level recency part block-idx state]}
;;   sorted by block-idx desc
;;   this is true of the live+nascent tries, and the garbage tries
;;   for correctness it's important that these invariants are maintained when a trie switches state
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
  (trie-state [trie-cat table]))

(def ^:const branch-factor 4)

(def ^:dynamic ^{:tag 'long} *file-size-target* (* 100 1024 1024))

(defn- map-while
  "map f until it returns nil, then append the rest of the collection unaltered.

   eagerly realises coll until the function returns nil, to avoid stack-overflow later.
   see #4377"
  [f coll]

  (loop [res (transient [])
         coll coll]
    (if-let [[x & xs] (seq coll)]
      (if-let [v (f x)]
        (recur (conj! res v) xs)
        (concat (persistent! res) coll))
      (concat (persistent! res) coll))))

(defn- stale-block-idx? [{:keys [live+nascent garbage] :as _tries} ^long block-idx]
  (when-let [{^long other-block-idx :block-idx} (or (first live+nascent) (first garbage))]
    (>= other-block-idx block-idx)))

(defn stale-msg?
  "messages have a total ordering, within their level/recency/part - so we know if we've received a message out-of-order.
   we check the staleness of messages so that we don't later have to try to insert a message within a list - we only ever need to prepend it"
  [{table-tries :tries} {:keys [^long level, part, recency, ^long block-idx]}]

  (stale-block-idx? (get table-tries [level recency part]) block-idx))

(defn- filter-garbage [tries]
  ((juxt filter remove) #(= (:state %) :garbage) tries))

(defn- supersede-partial-tries [{:keys [live+nascent garbage] :as tries}
                                {:keys [^long block-idx] :as _trie}
                                {:keys [^long file-size-target]}
                                as-of]
  (let [[new-garbage live+nascent] (->> live+nascent
                                        (map (fn [{^long other-block-idx :block-idx, ^long other-size :data-file-size, other-state :state, :as other-trie}]
                                               (cond-> other-trie
                                                 (and (= other-state :live)
                                                      (< other-size file-size-target)
                                                      (<= other-block-idx block-idx))
                                                 (-> (assoc :state :garbage
                                                            :garbage-as-of as-of)
                                                     (dissoc :trie-metadata)))))
                                        filter-garbage)]
    (-> tries
        (assoc :live+nascent (doall live+nascent))
        (assoc :garbage (doall (concat new-garbage garbage))))))


(defn- conj-trie [tries trie state]
  (let [trie (assoc trie :state state)
        {:keys [live+nascent garbage] :as tries} (or tries {:live+nascent (), :garbage ()})]
    (case state
      (:live :nascent) (assoc tries :live+nascent (conj live+nascent trie))
      :garbage (assoc tries :garbage (conj garbage trie)))))

(defn- insert-levelled-trie [tries trie trie-cat as-of]
  (-> tries
      (supersede-partial-tries trie trie-cat as-of)
      (conj-trie trie :live)))

(defn- supersede-by-block-idx [{:keys [live+nascent garbage] :as tries}, ^long block-idx as-of]
  (let [[new-garbage live+nascent] (->> live+nascent
                                        (map (fn [{^long other-block-idx :block-idx, :as trie}]
                                               (cond-> trie
                                                 (<= other-block-idx block-idx) (-> (assoc :state :garbage
                                                                                           :garbage-as-of as-of)
                                                                                    (dissoc :trie-metadata)))))
                                        filter-garbage)]
    (-> tries
        (assoc :live+nascent (doall live+nascent))
        (assoc :garbage (doall (concat new-garbage garbage))))))

(defn- sibling-tries [table-tries, {:keys [^long level, recency, part]}]
  (let [pop-part (pop part)]
    (for [p (range branch-factor)]
      (get table-tries [level recency (conj pop-part p)]))))

(defn- completed-part-group? [table-tries {:keys [^long block-idx] :as trie}]
  (->> (sibling-tries table-tries trie)
       (every? (fn [{:keys [live+nascent] :as _ln}]
                 (when-let [{^long ln-block-idx :block-idx, ln-state :state} (first live+nascent)]
                   (or (> ln-block-idx block-idx)
                       (= :nascent ln-state)))))))

(defn- mark-block-idx-live [tries ^long block-idx]
  (update tries :live+nascent
          #(doall (map (fn [{trie-state :state, trie-block-idx :block-idx, :as trie}]
                         (cond-> trie
                           (and (= trie-state :nascent)
                                (= trie-block-idx block-idx))
                           (assoc :state :live)))
                       %))))

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
                       (let [[{^long l1c-block-idx :block-idx, :as l1c}] (get-in table-cat [:tries [1 nil part] :live+nascent])]
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
                          (update [(dec level) recency (when (seq part) (pop part))] supersede-by-block-idx block-idx as-of)))))))))

(defn apply-trie-notification
  ([trie-cat table-cat trie]
   (apply-trie-notification trie-cat table-cat trie nil))
  ([trie-cat table-cat trie as-of]
   (let [trie (-> trie (update :part vec))]
     (cond-> table-cat
       (not (stale-msg? table-cat trie)) (insert-trie trie trie-cat as-of)))))

(defn current-tries [{:keys [tries]}]
  (->> tries
       (into [] (comp (mapcat (comp :live+nascent val))
                      (filter #(= (:state %) :live))))))

(defn all-tries [{:keys [tries]}]
  (->> (into [] (mapcat (comp (fn [{:keys [live+nascent garbage]}] (concat live+nascent garbage)) val)) tries)
       ;; the sort is needed as the table blocks need the current tries to be in the total order for restart
       (sort-by (juxt :level :block-idx #(or (:recency %) LocalDate/MAX)))))

(defn garbage-fn [as-of]
  (fn [{:keys [level garbage-as-of]}]
    (when (not= level 0)
      (<= (compare garbage-as-of as-of) 0))))

(defn garbage-tries [{:keys [tries]} as-of]
  (->> (mapcat (comp  :garbage val) tries)
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

(defrecord CatalogEntry [^LocalDate recency ^TrieMetadata trie-metadata ^TemporalBounds query-bounds]
  Metadata
  (testMetadata [_]
    (let [min-query-recency (min (.getLower (.getValidTime query-bounds)) (.getLower (.getSystemTime query-bounds)))]
      (if-let [^long recency (and recency (time/instant->micros (time/->instant recency {:default-tz ZoneOffset/UTC})))]
        ;; the recency of a trie is exclusive, no row in that file has a recency equal to it
        (< min-query-recency recency)
        true)))
  (getTemporalMetadata [_] (.getTemporalMetadata trie-metadata)))

(defn filter-tries [tries query-bounds]
  (-> (map (comp map->CatalogEntry #(assoc % :query-bounds query-bounds)) tries)
      (trie/filter-meta-objects query-bounds)))

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

(defrecord TrieCatalog [^Map !table-cats, ^long file-size-target]
  xtdb.trie.TrieCatalog
  (addTries [this table added-tries as-of]
    (.compute !table-cats table
              (fn [_table tries]
                (reduce (fn [table-cat ^TrieDetails added-trie]
                          (if-let [parsed-key (trie/parse-trie-key (.getTrieKey added-trie))]
                            (apply-trie-notification this table-cat
                                                     (-> parsed-key
                                                         (assoc :data-file-size (.getDataFileSize added-trie)
                                                                :trie-metadata (.getTrieMetadata added-trie)))
                                                     as-of)
                            table-cat))
                        (or tries {})
                        added-tries))))

  (getTables [_] (set (keys !table-cats)))

  (garbageTries [_ table as-of]
    (->> (garbage-tries (.get !table-cats table) as-of)
         (into #{} (map :trie-key))))

  (deleteTries [_ table garbage-trie-keys]
    (.compute !table-cats table
              (fn [_table tries]
                (remove-garbage tries garbage-trie-keys))))

  PTrieCatalog
  (trie-state [_ table] (.get !table-cats table)))

(defmethod ig/prep-key :xtdb/trie-catalog [_ opts]
  (into {:buffer-pool (ig/ref :xtdb/buffer-pool)
         :block-cat (ig/ref :xtdb/block-catalog)
         :table-cat (ig/ref :xtdb/table-catalog)}
        opts))

(defn new-trie-details? [^TrieDetails trie-details]
  (.hasTrieState trie-details))

(defn trie-catalog-init [table->table-block]
  (if (some-> table->table-block first val :tries first new-trie-details?)
    (let [!table-cats (ConcurrentHashMap.)]
      (doseq [[table {:keys [tries]}] table->table-block
              :let [tries (-> (reduce (fn [table-cat ^TrieDetails added-trie]
                                        (let [{:keys [level recency part state] :as trie} (trie/<-trie-details added-trie)]
                                          (update table-cat [level recency part] conj-trie trie state)))
                                      {}
                                      tries)
                              (update-vals (fn [tries]
                                             (update-vals tries #(sort-by :block-idx (fn [a b] (compare b a)) %)))))]]
        (.put !table-cats table {:tries tries}))

      (TrieCatalog. !table-cats *file-size-target*))

    ;; TODO: This else statement is here to support block files that have not yet passed to the new extended TrieDetails format
    ;; see #4526
    (let [cat (TrieCatalog. (ConcurrentHashMap.) *file-size-target*)
          now (Instant/now)]
      (doseq [[table {:keys [tries]}] table->table-block]
        (.addTries cat table tries now))
      cat)))

(defmethod ig/init-key :xtdb/trie-catalog [_ {:keys [^BufferPool buffer-pool, ^BlockCatalog block-cat]}]
  (log/debug "starting trie catalog...")
  (let [[_ table->table-block] (table-cat/load-tables-to-metadata buffer-pool block-cat)
        cat (trie-catalog-init table->table-block)]
    (log/debug "trie catalog started")
    cat))

(defn trie-catalog ^xtdb.trie.TrieCatalog [node]
  (util/component node :xtdb/trie-catalog))
