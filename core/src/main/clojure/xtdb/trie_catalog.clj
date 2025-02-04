(ns xtdb.trie-catalog
  (:require [integrant.core :as ig]
            [xtdb.metadata :as meta]
            [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import [java.util Map]
           [java.util.concurrent ConcurrentHashMap]
           (xtdb BufferPool)
           xtdb.api.storage.ObjectStore$StoredObject
           xtdb.log.proto.AddedTrie
           xtdb.metadata.IMetadataManager))

(defprotocol PTrieCatalog
  (table-names [trie-cat])
  (trie-state [trie-cat table-name]))

(def ^:const branch-factor 4)

(def ^:dynamic *l1-row-count-limit* (* 100 1024))

(defn ->added-trie
  [table-name, trie-key, ^long data-file-size]

  (.. (AddedTrie/newBuilder)
      (setTableName table-name)
      (setTrieKey trie-key)
      (setDataFileSize data-file-size)
      (build)))

(defn- superseded-l0-trie? [table-tries {:keys [block-idx]}]
  (or (when-let [{l0-block-idx :block-idx} (first (get table-tries [0 []]))]
        (>= l0-block-idx block-idx))

      (when-let [{l1-block-idx :block-idx} (first (get table-tries [1 []]))]
        (>= l1-block-idx block-idx))))

(defn- superseded-l1-trie? [table-tries {:keys [block-idx]}]
  (or (when-let [{l1-block-idx :block-idx} (first (get table-tries [1 []]))]
        (>= l1-block-idx block-idx))

      (->> (map table-tries (for [p (range branch-factor)]
                              [2 [p]]))
           (every? (comp (fn [{l2-block-idx :block-idx, :as l2-trie}]
                           (and l2-trie
                                (>= l2-block-idx block-idx)))
                         first)))))

(defn superseded-ln-trie [table-tries {:keys [level block-idx part]}]
  (or (when-let [{ln-block-idx :block-idx} (first (get table-tries [level part]))]
        (>= ln-block-idx block-idx))

      (->> (map table-tries (for [p (range branch-factor)]
                              [(inc level) (conj part p)]))
           (every? (comp (fn [{lnp1-block-idx :block-idx, :as lnp1-trie}]
                           (when lnp1-trie
                             (>= lnp1-block-idx block-idx)))
                         first)))))

(defn- superseded-trie? [table-tries {:keys [level] :as trie}]
  (case (long level)
    0 (superseded-l0-trie? table-tries trie)
    1 (superseded-l1-trie? table-tries trie)
    (superseded-ln-trie table-tries trie)))

(defn- supersede-partial-l1-tries [l1-tries {:keys [block-idx]} {:keys [l1-row-count-limit]}]
  (->> l1-tries
       (map (fn [{l1-block-idx :block-idx, l1-rows :rows, l1-state :state, :as l1-trie}]
              (cond-> l1-trie
                (and (= l1-state :live)
                     (< l1-rows l1-row-count-limit)
                     (>= block-idx l1-block-idx))
                (assoc :state :garbage))))))

(defn- supersede-l0-tries [l0-tries {:keys [block-idx]}]
  (->> l0-tries
       (map (fn [{l0-block-idx :block-idx, :as l0-trie}]
              (cond-> l0-trie
                (<= l0-block-idx block-idx)
                (assoc :state :garbage))))))

(defn- conj-nascent-ln-trie [table-tries {:keys [level part] :as trie}]
  (-> table-tries
      (update [level part]
              (fnil (fn [ln-part-tries]
                      (conj ln-part-tries (assoc trie :state :nascent)))
                    '()))))

(defn- completed-ln-group? [table-tries {:keys [level block-idx part]}]
  (->> (let [pop-part (pop part)]
         (for [p (range branch-factor)]
           [level (conj pop-part p)]))
       (map (comp first table-tries))

       (every? (fn [{ln-block-idx :block-idx, ln-state :state, :as ln-trie}]
                 (and ln-trie
                      (or (> ln-block-idx block-idx)
                          (= :nascent ln-state)))))))

(defn- mark-ln-group-live [table-tries {:keys [block-idx level part]}]
  (reduce (fn [table-tries part]
            (-> table-tries
                (update part
                        (fn [ln-tries]
                          (->> ln-tries
                               (map (fn [{ln-state :state, ln-block-idx :block-idx, :as ln-trie}]
                                      (cond-> ln-trie
                                        (and (= ln-state :nascent)
                                             (= ln-block-idx block-idx))
                                        (assoc :state :live)))))))))
          table-tries
          (let [pop-part (pop part)]
            (for [p (range branch-factor)]
              [level (conj pop-part p)]))))

(defn- supersede-lnm1-tries [table-tries {:keys [block-idx level part]}]
  (-> table-tries
      (update [(dec level) (pop part)]
              (fn [lnm1-tries]
                (->> lnm1-tries
                     (map (fn [{lnm1-state :state, lnm1-block-idx :block-idx, :as lnm1-trie}]
                            (cond-> lnm1-trie
                              (and (= lnm1-state :live)
                                   (<= lnm1-block-idx block-idx))
                              (assoc :state :garbage)))))))))

(defn- conj-trie [table-tries {:keys [level] :as trie} trie-cat]
  (case (long level)
    0 (-> table-tries
          (update [0 []] (fnil conj '())
                  (assoc trie :state :live)))

    1 (-> table-tries
          (update [1 []] (fnil (fn [l1-tries trie]
                                 (-> l1-tries
                                     (supersede-partial-l1-tries trie trie-cat)
                                     (conj (assoc trie :state :live))))
                               '())
                  trie)
          (update [0 []] supersede-l0-tries trie))

    (as-> table-tries table-tries
      (conj-nascent-ln-trie table-tries trie)

      (cond-> table-tries
        (completed-ln-group? table-tries trie) (-> (mark-ln-group-live trie)
                                                   (supersede-lnm1-tries trie))))))

(defn apply-trie-notification
  ([_trie-cat] {})
  ([trie-cat tries ^AddedTrie added-trie]
   (let [trie (-> (trie/parse-trie-key (.getTrieKey added-trie))
                  (update :part vec)
                  (assoc :data-file-size (.getDataFileSize added-trie)))]
     (cond-> tries
       (not (superseded-trie? tries trie)) (conj-trie trie trie-cat))))

  ([_trie-cat tries] tries))

(defn current-tries [tries]
  (->> tries
       (into [] (comp (mapcat val)
                      (filter #(= (:state %) :live))))))

(defrecord TrieCatalog [^Map !table-tries, ^long l1-row-count-limit]
  xtdb.trie.TrieCatalog
  (addTrie [this added-trie]
    (.compute !table-tries (.getTableName added-trie)
              (fn [_table-name tries]
                (apply-trie-notification this tries added-trie))))

  PTrieCatalog
  (table-names [_] (set (keys !table-tries)))

  (trie-state [_ table-name] (.get !table-tries table-name)))

(defmethod ig/prep-key :xtdb/trie-catalog [_ opts]
  (into {:buffer-pool (ig/ref :xtdb/buffer-pool)
         :metadata-mgr (ig/ref ::meta/metadata-manager)}
        opts))

(defmethod ig/init-key :xtdb/trie-catalog [_ {:keys [^BufferPool buffer-pool, ^IMetadataManager metadata-mgr]}]
  (let [!table-tries (ConcurrentHashMap.)
        trie-cat (->TrieCatalog !table-tries *l1-row-count-limit*)]

    (doseq [table-name (.allTableNames metadata-mgr)]
      (.put !table-tries table-name
            (->> (.listAllObjects buffer-pool (trie/->table-meta-dir table-name))
                 (transduce (map (fn [^ObjectStore$StoredObject obj]
                                   (->added-trie table-name
                                                 (str (.getFileName (.getKey obj)))
                                                 (.getSize obj))))
                            (partial apply-trie-notification trie-cat)))))

    trie-cat))

(defn trie-catalog ^xtdb.trie.TrieCatalog [node]
  (util/component node :xtdb/trie-catalog))
