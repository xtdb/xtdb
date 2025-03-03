(ns xtdb.trie-catalog
  (:require [integrant.core :as ig]
            [xtdb.trie :as trie]
            [xtdb.util :as util])
  (:import [java.util Map]
           [java.util.concurrent ConcurrentHashMap]
           (xtdb BufferPool)
           xtdb.api.storage.ObjectStore$StoredObject
           xtdb.catalog.BlockCatalog
           xtdb.log.proto.AddedTrie))

(defprotocol PTrieCatalog
  (trie-state [trie-cat table-name]))

(def ^:const branch-factor 4)

(def ^:dynamic *l1-size-limit* (* 100 1024 1024))

(defn ->added-trie ^xtdb.log.proto.AddedTrie [table-name, trie-key, ^long data-file-size]
  (.. (AddedTrie/newBuilder)
      (setTableName table-name)
      (setTrieKey trie-key)
      (setDataFileSize data-file-size)
      (build)))

(defn- superseded-l0-trie? [{:keys [l0-tries l1-tries]} {:keys [block-idx]}]
  (or (when-let [{l0-block-idx :block-idx} (first l0-tries)]
        (>= l0-block-idx block-idx))

      (when-let [{l1-block-idx :block-idx} (first l1-tries)]
        (>= l1-block-idx block-idx))))

(defn- superseded-l1-trie? [{:keys [l1-tries ln-tries]} {:keys [block-idx]}]
  (or (when-let [{l1-block-idx :block-idx} (first l1-tries)]
        (>= l1-block-idx block-idx))

      (->> (map (or ln-tries {})
                (for [p (range branch-factor)]
                  [2 [p]]))
           (every? (comp (fn [{l2-block-idx :block-idx, :as l2-trie}]
                           (and l2-trie
                                (>= l2-block-idx block-idx)))
                         first)))))

(defn superseded-ln-trie [{:keys [ln-tries]} {:keys [level block-idx part]}]
  (or (when-let [{ln-block-idx :block-idx} (first (get ln-tries [level part]))]
        (>= ln-block-idx block-idx))

      (->> (map (or ln-tries {})
                (for [p (range branch-factor)]
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

(defn- supersede-partial-l1-tries [l1-tries {:keys [block-idx]} {:keys [l1-size-limit]}]
  (->> (or l1-tries {})
       (map (fn [{l1-block-idx :block-idx, l1-size :data-file-size, l1-state :state, :as l1-trie}]
              (cond-> l1-trie
                (and (= l1-state :live)
                     (< l1-size l1-size-limit)
                     (>= block-idx l1-block-idx))
                (assoc :state :garbage))))))

(defn- supersede-l0-tries [l0-tries {:keys [block-idx]}]
  (->> l0-tries
       (map (fn [{l0-block-idx :block-idx, :as l0-trie}]
              (cond-> l0-trie
                (<= l0-block-idx block-idx)
                (assoc :state :garbage))))))

(defn- conj-nascent-ln-trie [ln-tries {:keys [level part] :as trie}]
  (-> ln-tries
      (update [level part]
              (fnil (fn [ln-part-tries]
                      (conj ln-part-tries (assoc trie :state :nascent)))
                    '()))))

(defn- completed-ln-group? [{:keys [ln-tries]} {:keys [level block-idx part]}]
  (->> (map (comp first (or ln-tries {}))
            (let [pop-part (pop part)]
              (for [p (range branch-factor)]
                [level (conj pop-part p)])))

       (every? (fn [{ln-block-idx :block-idx, ln-state :state, :as ln-trie}]
                 (and ln-trie
                      (or (> ln-block-idx block-idx)
                          (= :nascent ln-state)))))))

(defn- mark-ln-group-live [ln-tries {:keys [block-idx level part]}]
  (reduce (fn [ln-tries part]
            (-> ln-tries
                (update part
                        (fn [ln-part-tries]
                          (->> ln-part-tries
                               (map (fn [{ln-state :state, ln-block-idx :block-idx, :as ln-trie}]
                                      (cond-> ln-trie
                                        (and (= ln-state :nascent)
                                             (= ln-block-idx block-idx))
                                        (assoc :state :live)))))))))
          ln-tries
          (let [pop-part (pop part)]
            (for [p (range branch-factor)]
              [level (conj pop-part p)]))))

(defn- supersede-lnm1-tries [table-tries {:keys [block-idx level part]}]
  (-> table-tries
      (update-in (if (= level 2)
                   [:l1-tries]
                   [:ln-tries [(dec level) (pop part)]])
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
          (update :l0-tries (fnil conj '()) (assoc trie :state :live)))

    1 (-> table-tries
          (update :l1-tries
                  (fnil (fn [l1-tries trie]
                          (-> l1-tries
                              (supersede-partial-l1-tries trie trie-cat)
                              (conj (assoc trie :state :live))))
                        '())
                  trie)
          (update :l0-tries supersede-l0-tries trie))

    (-> table-tries
        (update :ln-tries conj-nascent-ln-trie trie)
        (as-> table-tries (cond-> table-tries
                            (completed-ln-group? table-tries trie)
                            (-> (update :ln-tries mark-ln-group-live trie)
                                (supersede-lnm1-tries trie)))))))

(defn apply-trie-notification [trie-cat tries trie]
  (let [trie (-> trie
                 (update :part vec))]
    (cond-> tries
      (not (superseded-trie? tries trie)) (conj-trie trie trie-cat))))

(defn current-tries [{:keys [l0-tries l1-tries ln-tries]}]
  (->> (concat l0-tries l1-tries (sequence cat (vals ln-tries)))
       (into [] (filter #(= (:state %) :live)))
       (sort-by :block-idx)))

(defrecord TrieCatalog [^Map !table-tries, ^long l1-size-limit]
  xtdb.trie.TrieCatalog
  (addTries [this added-tries]
    (doseq [[table-name added-tries] (->> added-tries
                                          (group-by #(.getTableName ^AddedTrie %)))]
      (.compute !table-tries table-name
                (fn [_table-name tries]
                  (reduce (fn [table-tries ^AddedTrie added-trie]
                            (if-let [parsed-key (trie/parse-trie-key (.getTrieKey added-trie))]
                              (apply-trie-notification this table-tries
                                                       (-> parsed-key
                                                           (update :part vec)
                                                           (assoc :data-file-size (.getDataFileSize added-trie))))
                              table-tries))
                          (or tries {})
                          added-tries)))))

  (getTableNames [_] (set (keys !table-tries)))

  PTrieCatalog
  (trie-state [_ table-name] (.get !table-tries table-name)))

(defmethod ig/prep-key :xtdb/trie-catalog [_ opts]
  (into {:buffer-pool (ig/ref :xtdb/buffer-pool)
         :block-cat (ig/ref :xtdb/block-catalog)}
        opts))

(defmethod ig/init-key :xtdb/trie-catalog [_ {:keys [^BufferPool buffer-pool, ^BlockCatalog block-cat]}]
  (doto (TrieCatalog. (ConcurrentHashMap.) *l1-size-limit*)
    (.addTries (for [table-name (.getAllTableNames block-cat)
                     ^ObjectStore$StoredObject obj (.listAllObjects buffer-pool (trie/->table-meta-dir table-name))
                     :let [file-name (str (.getFileName (.getKey obj)))
                           [_ trie-key] (re-matches #"(.+)\.arrow" file-name)]
                     :when trie-key]
                 (->added-trie table-name trie-key (.getSize obj))))))

(defn trie-catalog ^xtdb.trie.TrieCatalog [node]
  (util/component node :xtdb/trie-catalog))
