(ns xtdb.trie
  (:require [xtdb.buffer-pool]
            [xtdb.table :as table]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import com.carrotsearch.hppc.ByteArrayList
           (java.nio.file Path)
           (java.time LocalDate)
           (java.util ArrayList)
           (xtdb.log.proto TrieDetails TrieMetadata TrieState)
           (xtdb.segment Segment$PageMeta)
           (xtdb.trie MemoryHashTrie MemoryHashTrie$Branch MemoryHashTrie$Leaf MemoryHashTrie$Node Trie Trie$Key)
           (xtdb.util Temporal TemporalBounds TemporalDimension)))

(defn ->trie-details ^TrieDetails
  ([table {:keys [trie-key, ^long data-file-size, ^TrieMetadata trie-metadata state garbage-as-of]}]
   (-> (TrieDetails/newBuilder)
       (.setTableName (str (table/ref->schema+table table)))
       (.setTrieKey trie-key)
       (.setDataFileSize data-file-size)
       (cond-> trie-metadata (.setTrieMetadata trie-metadata))
       (cond-> state (.setTrieState (case state
                                      :live TrieState/LIVE
                                      :nascent TrieState/NASCENT
                                      :garbage TrieState/GARBAGE)))
       (cond-> garbage-as-of (.setGarbageAsOf (time/instant->micros garbage-as-of)))
       (.build)))
  ([table, trie-key, data-file-size]
   (->trie-details table trie-key data-file-size nil))
  ([table, trie-key, data-file-size, ^TrieMetadata trie-metadata]
   (->trie-details table trie-key data-file-size trie-metadata nil))
  ([table, trie-key, data-file-size, ^TrieMetadata trie-metadata state]
   (->trie-details table {:trie-key trie-key
                          :data-file-size data-file-size
                          :trie-metadata trie-metadata
                          :state state})))

(declare parse-trie-key)

(defn <-trie-details [^TrieDetails trie-details]
  (merge
   {:live (), :garbage ()}
   (cond-> {:table-name (.getTableName trie-details)
            :data-file-size (.getDataFileSize trie-details)}

     (.hasTrieMetadata trie-details)
     (assoc :trie-metadata (.getTrieMetadata trie-details))

     (.hasTrieState trie-details)
     (assoc :state (condp = (.getTrieState trie-details)
                     TrieState/LIVE :live
                     TrieState/NASCENT :nascent
                     TrieState/GARBAGE :garbage))

     (.hasGarbageAsOf trie-details)
     (assoc :garbage-as-of (time/micros->instant (.getGarbageAsOf trie-details))))
   (parse-trie-key (.getTrieKey trie-details))))

(defn ->trie-key [^long level, ^LocalDate recency, ^bytes part, ^long block-idx]
  (str (Trie$Key. level recency (some-> part ByteArrayList/from) block-idx)))

(defn ->l0-trie-key ^java.lang.String [^long block-idx]
  (->trie-key 0 nil nil block-idx))

(defn ->l1-trie-key ^java.lang.String [^LocalDate recency, ^long block-idx]
  (->trie-key 1 recency nil block-idx))

(defn parse-trie-key [trie-key]
  (try
    (let [k (Trie/parseKey trie-key)]
      {:trie-key trie-key
       :level (.getLevel k)
       :recency (.getRecency k)
       :part (or (some-> (.getPart k) (.toArray) vec) [])
       :block-idx (.getBlockIndex k)})
    (catch IllegalArgumentException _)
    (catch IllegalStateException _)))

(defn parse-trie-file-path [^Path file-path]
  (-> (parse-trie-key (str (.getFileName file-path)))
      (assoc :file-path file-path)))

(defn table-name->table-path ^java.nio.file.Path [^String table-name]
  (Trie/getTablePath table-name))

(defn ->table-meta-dir ^java.nio.file.Path [^String table-name]
  (Trie/metaFileDir table-name))

(defn ->live-trie ^MemoryHashTrie [log-limit page-limit iid-rdr]
  (-> (doto (MemoryHashTrie/builder iid-rdr)
        (.setLogLimit log-limit)
        (.setPageLimit page-limit))
      (.build)))

(defn filter-pages
  ([pages] (filter-pages pages {}))
  ([pages {:keys [^TemporalBounds query-bounds], :or {query-bounds (TemporalBounds.)}}]
   (let [leaves (ArrayList.)]
     (loop [[^Segment$PageMeta page & more-pages] pages
            smallest-valid-from Long/MAX_VALUE
            largest-valid-to Long/MIN_VALUE
            smallest-system-from Long/MAX_VALUE
            non-taken-pages []]
       (if page
         (let [temporal-metadata (.getTemporalMetadata page)
               take-node? (and (Temporal/intersects temporal-metadata query-bounds)
                               (.testMetadata page))]
           (if take-node?
             (do
               (.add leaves page)
               (recur more-pages
                      (min smallest-valid-from (.getMinValidFrom temporal-metadata))
                      (max largest-valid-to (.getMaxValidTo temporal-metadata))
                      (min smallest-system-from (.getMinSystemFrom temporal-metadata))
                      non-taken-pages))

             (recur more-pages
                    smallest-valid-from
                    largest-valid-to
                    smallest-system-from
                    (cond-> non-taken-pages
                      (Temporal/intersectsSystemTime temporal-metadata query-bounds)
                      (conj page)))))

         (when (seq leaves)
           (let [valid-time (TemporalDimension. smallest-valid-from largest-valid-to)]
             (doseq [^Segment$PageMeta page non-taken-pages]
               (let [temporal-metadata (.getTemporalMetadata page)
                     obj-largest-system-from (.getMaxSystemFrom temporal-metadata)
                     page-recency (.getRecency page)]
                 (when (and (<= smallest-system-from obj-largest-system-from)
                            (.intersects (TemporalDimension. (.getMinValidFrom temporal-metadata)
                                                             (.getMaxValidTo temporal-metadata))
                                         valid-time)
                            (or (>= page-recency smallest-valid-from)
                                (>= page-recency smallest-system-from)))
                   (.add leaves page)))))
           (vec leaves)))))))

(defn- <-MemoryHashTrieNode [^MemoryHashTrie$Node node]
  (when node
    (condp instance? node
      MemoryHashTrie$Leaf
      [:leaf (vec (.getData ^MemoryHashTrie$Leaf node))]

      MemoryHashTrie$Branch
      (into [:branch] (map <-MemoryHashTrieNode) (.getHashChildren ^MemoryHashTrie$Branch node)))))

(defn <-MemoryHashTrie [^MemoryHashTrie trie]
  {:log-limit (.getLogLimit trie)
   :page-limit (.getPageLimit trie)
   :tree (<-MemoryHashTrieNode (.getRootNode trie))})
