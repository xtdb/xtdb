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

(defn ->added-trie
  [table-name, trie-key, ^long data-file-size]

  (.. (AddedTrie/newBuilder)
      (setTableName table-name)
      (setTrieKey trie-key)
      (setDataFileSize data-file-size)
      (build)))

(defn- superseded-l0-trie? [table-tries {:keys [next-row]}]
  (or (when-let [{l0-next-row :next-row} (first (get table-tries [0 []]))]
        (>= l0-next-row next-row))

      (when-let [{l1-next-row :next-row} (first (get table-tries [1 []]))]
        (>= l1-next-row next-row))))

(defn- superseded-l1-trie? [table-tries {:keys [next-row first-row]}]
  (or (when-let [{l1-next-row :next-row, l1-first-row :first-row} (first (get table-tries [1 []]))]
        (or (> l1-first-row first-row)
            (>= l1-next-row next-row)))

      (->> (map table-tries (for [p (range branch-factor)]
                              [2 [p]]))
           (every? (comp (fn [{l2-next-row :next-row, :as l2-trie}]
                           (and l2-trie
                                (>= l2-next-row next-row)))
                         first)))))

(defn superseded-ln-trie [table-tries {:keys [level next-row part]}]
  (or (when-let [{ln-next-row :next-row} (first (get table-tries [level part]))]
        (>= ln-next-row next-row))

      (->> (map table-tries (for [p (range branch-factor)]
                              [(inc level) (conj part p)]))
           (every? (comp (fn [{lnp1-next-row :next-row, :as lnp1-trie}]
                           (when lnp1-trie
                             (>= lnp1-next-row next-row)))
                         first)))))

(defn- superseded-trie? [table-tries {:keys [level] :as trie}]
  (case (long level)
    0 (superseded-l0-trie? table-tries trie)
    1 (superseded-l1-trie? table-tries trie)
    (superseded-ln-trie table-tries trie)))

(defn- supersede-partial-l1-tries [l1-tries {:keys [first-row next-row]}]
  (->> l1-tries
       (map (fn [{l1-first-row :first-row, l1-next-row :next-row, l1-state :state, :as l1-trie}]
              (cond-> l1-trie
                (and (= l1-state :live)
                     (= first-row l1-first-row)
                     (>= next-row l1-next-row))
                (assoc :state :garbage))))))

(defn- supersede-l0-tries [l0-tries {:keys [next-row]}]
  (->> l0-tries
       (map (fn [{l0-next-row :next-row, :as l0-trie}]
              (cond-> l0-trie
                (<= l0-next-row next-row)
                (assoc :state :garbage))))))

(defn- conj-nascent-ln-trie [table-tries {:keys [level part] :as trie}]
  (-> table-tries
      (update [level part]
              (fnil (fn [ln-part-tries]
                      (conj ln-part-tries (assoc trie :state :nascent)))
                    '()))))

(defn- completed-ln-group? [table-tries {:keys [level next-row part]}]
  (->> (let [pop-part (pop part)]
         (for [p (range branch-factor)]
           [level (conj pop-part p)]))
       (map (comp first table-tries))

       (every? (fn [{ln-next-row :next-row, ln-state :state, :as ln-trie}]
                 (and ln-trie
                      (or (> ln-next-row next-row)
                          (= :nascent ln-state)))))))

(defn- mark-ln-group-live [table-tries {:keys [next-row level part]}]
  (reduce (fn [table-tries part]
            (-> table-tries
                (update part
                        (fn [ln-tries]
                          (->> ln-tries
                               (map (fn [{ln-state :state, ln-next-row :next-row, :as ln-trie}]
                                      (cond-> ln-trie
                                        (and (= ln-state :nascent)
                                             (= ln-next-row next-row))
                                        (assoc :state :live)))))))))
          table-tries
          (let [pop-part (pop part)]
            (for [p (range branch-factor)]
              [level (conj pop-part p)]))))

(defn- supersede-lnm1-tries [table-tries {:keys [next-row level part]}]
  (-> table-tries
      (update [(dec level) (pop part)]
              (fn [lnm1-tries]
                (->> lnm1-tries
                     (map (fn [{lnm1-state :state, lnm1-next-row :next-row, :as lnm1-trie}]
                            (cond-> lnm1-trie
                              (and (= lnm1-state :live)
                                   (<= lnm1-next-row next-row))
                              (assoc :state :garbage)))))))))

(defn- conj-trie [table-tries {:keys [level] :as trie}]
  (case (long level)
    0 (-> table-tries
          (update [0 []] (fnil conj '())
                  (assoc trie :state :live)))

    1 (-> table-tries
          (update [1 []] (fnil (fn [l1-tries trie]
                                 (-> l1-tries
                                     (supersede-partial-l1-tries trie)
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
  ([] {})
  ([tries ^AddedTrie added-trie]
   (let [trie (-> (trie/parse-trie-key (.getTrieKey added-trie))
                  (update :part vec)
                  (assoc :data-file-size (.getDataFileSize added-trie)))]
     (cond-> tries
       (not (superseded-trie? tries trie)) (conj-trie trie))))

  ([tries] tries))

(defn current-tries [tries]
  (->> tries
       (into [] (comp (mapcat val)
                      (filter #(= (:state %) :live))))))

(defrecord TrieCatalog [^Map !table-tries]
  xtdb.trie.TrieCatalog
  (addTrie [_ added-trie]
    (.compute !table-tries (.getTableName added-trie)
              (fn [_table-name tries]
                (apply-trie-notification tries added-trie))))

  PTrieCatalog
  (table-names [_] (set (keys !table-tries)))

  (trie-state [_ table-name] (.get !table-tries table-name)))

(defmethod ig/prep-key :xtdb/trie-catalog [_ opts]
  (into {:buffer-pool (ig/ref :xtdb/buffer-pool)
         :metadata-mgr (ig/ref ::meta/metadata-manager)}
        opts))

(defmethod ig/init-key :xtdb/trie-catalog [_ {:keys [^BufferPool buffer-pool, ^IMetadataManager metadata-mgr]}]
  (let [!table-tries (ConcurrentHashMap.)]
    (doseq [table-name (.allTableNames metadata-mgr)]
      (.put !table-tries table-name
            (->> (.listAllObjects buffer-pool (trie/->table-meta-dir table-name))
                 (transduce (map (fn [^ObjectStore$StoredObject obj]
                                   (->added-trie table-name
                                                 (str (.getFileName (.getKey obj)))
                                                 (.getSize obj))))
                            apply-trie-notification))))

    (->TrieCatalog !table-tries)))

(defn trie-catalog ^xtdb.trie.TrieCatalog [node]
  (util/component node :xtdb/trie-catalog))
