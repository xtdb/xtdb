(ns xtdb.trie
  (:require [xtdb.table :as table]
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

(defn ->table-block-trie-details ^TrieDetails
  ;; In-block companion to ->trie-details. Builds a TrieDetails without table_name,
  ;; since inside a block the table is already fixed by the file path. Saves the
  ;; per-trie name bytes on garbage-heavy tables — see #5512.
  [{:keys [trie-key, ^long data-file-size, ^TrieMetadata trie-metadata state garbage-as-of]}]
  (-> (TrieDetails/newBuilder)
      (.setTrieKey trie-key)
      (.setDataFileSize data-file-size)
      (cond-> trie-metadata (.setTrieMetadata trie-metadata))
      (cond-> state (.setTrieState (case state
                                     :live TrieState/LIVE
                                     :nascent TrieState/NASCENT
                                     :garbage TrieState/GARBAGE)))
      (cond-> garbage-as-of (.setGarbageAsOf (time/instant->micros garbage-as-of)))
      (.build)))

(declare parse-trie-key)

(defn <-trie-details [^TrieDetails trie-details]
  (merge
   {:live (), :garbage ()}
   (cond-> {:table-name (.getTableName trie-details)
            :data-file-size (.getDataFileSize trie-details)}

     (.hasTrieMetadata trie-details)
     ;; temporarily (remove after 2.2): clear existing IID blooms (#5355)
     (assoc :trie-metadata (-> (.getTrieMetadata trie-details) .toBuilder .clearIidBloom .build))

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

(defn ->live-trie ^MemoryHashTrie [log-limit page-limit iid-rdr]
  (-> (doto (MemoryHashTrie/builder iid-rdr)
        (.setLogLimit log-limit)
        (.setPageLimit page-limit))
      (.build)))

(defn filter-pages [pages {:keys [^TemporalBounds query-bounds projects-temporal-cols? clamp-valid-time?]}]
  ;; Pages are categorised by what they can do to the result:
  ;;
  ;; - **emit** — rows in this page can appear in the output.
  ;;   Temporal bounds match the query AND content predicate (`testMetadata`) matches.
  ;; - **supersede** — rows here can affect an emit row's multiplicity - whether it's duplicated (U-shaped) or filtered out.
  ;;   Temporal bounds match the query; content predicate need not.
  ;; - **constrain** — rows here can affect the bounds of the resulting polygons.
  ;;   Temporal bounds disjoint from the query but overlapping the emit envelope.
  ;;   We exclude these if the caller doesn't observe their contribution: either the query doesn't
  ;;   project temporal columns, or it projects them clamped to the query bounds.

  (let [leaves (ArrayList.)
        min-query-recency (long (min (.getLower (.getValidTime query-bounds))
                                     (.getLower (.getSystemTime query-bounds))))
        temporal-intersects? (fn [^Segment$PageMeta page]
                               (and (Temporal/intersects (.getTemporalMetadata page) query-bounds)
                                    (< min-query-recency (.getRecency page))))]

    (loop [[^Segment$PageMeta page & more-pages] pages
           smallest-valid-from Long/MAX_VALUE
           largest-valid-to Long/MIN_VALUE
           smallest-system-from Long/MAX_VALUE
           non-emit-candidates []]
      (if page
        ;; Phase 1 — partition pages into the emit set (in result) and non-emit candidates
        (let [temporal-metadata (.getTemporalMetadata page)]
          (if (and (temporal-intersects? page) (.testMetadata page))
            (do
              (.add leaves page)
              (recur more-pages
                     (min smallest-valid-from (.getMinValidFrom temporal-metadata))
                     (max largest-valid-to (.getMaxValidTo temporal-metadata))
                     (min smallest-system-from (.getMinSystemFrom temporal-metadata))
                     non-emit-candidates))

            (recur more-pages
                   smallest-valid-from largest-valid-to smallest-system-from
                   (cond-> non-emit-candidates
                     (Temporal/intersectsSystemTime temporal-metadata query-bounds)
                     (conj page)))))

        (when (seq leaves)
          ;; Phase 2 - include all of the pages that may affect the rows in phase 1.
          (let [envelope-vt (TemporalDimension. smallest-valid-from largest-valid-to)]
            (doseq [^Segment$PageMeta page non-emit-candidates
                    :let [tm (.getTemporalMetadata page)
                          page-recency (.getRecency page)]
                    :when (and
                            ;; if the max system-from of the candidate is before the min system-from of the emit set,
                            ;; none of the rows in that file can affect the rows in the emit set.
                            (<= smallest-system-from (.getMaxSystemFrom tm))

                            (if (and projects-temporal-cols? (not clamp-valid-time?))
                              ;; keep candidates whose vt overlaps the emit envelope
                              ;; (covers both supersede and constrain — the latter feed `_valid_from` etc.).
                              (and (.intersects (TemporalDimension. (.getMinValidFrom tm) (.getMaxValidTo tm))
                                                envelope-vt)
                                   (or (>= page-recency smallest-valid-from)
                                       (>= page-recency smallest-system-from)))

                              ;; keep only candidates that vt-intersect the query itself
                              ;; (supersede only; constrain-only contribution is wasted I/O —
                              ;; either nobody reads vt cols, or their contribution would be clamped away)
                              (temporal-intersects? page)))]

              (.add leaves page)))

          (vec leaves))))))

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
