(ns xtdb.trie
  (:require [xtdb.buffer-pool]
            [xtdb.util :as util])
  (:import com.carrotsearch.hppc.ByteArrayList
           (java.nio.file Path)
           java.time.LocalDate
           (java.util ArrayList)
           (xtdb.log.proto TrieDetails TrieMetadata)
           xtdb.operator.scan.MergePlanPage
           (xtdb.trie ISegment MemoryHashTrie Trie Trie$Key)
           (xtdb.util Temporal TemporalBounds TemporalDimension)))

(defn ->trie-details ^TrieDetails
  ([table-name, trie-key, ^long data-file-size]
   (.. (TrieDetails/newBuilder)
       (setTableName table-name)
       (setTrieKey trie-key)
       (setDataFileSize data-file-size)
       (build)))
  ([table-name, trie-key, ^long data-file-size, ^TrieMetadata trie-metadata]
   (.. (TrieDetails/newBuilder)
       (setTableName table-name)
       (setTrieKey trie-key)
       (setDataFileSize data-file-size)
       (setTrieMetadata trie-metadata)
       (build))))

(defn ->trie-key [^long level, ^LocalDate recency, ^bytes part, ^long block-idx]
  (str (Trie$Key. level recency (some-> part ByteArrayList/from) block-idx)))

(defn ->l0-trie-key [^long block-idx]
  (->trie-key 0 nil nil block-idx))

(defn ->l1-trie-key [^LocalDate recency, ^long block-idx]
  (->trie-key 1 recency nil block-idx))

(defn parse-trie-key [trie-key]
  (try
    (let [k (Trie/parseKey trie-key)]
      {:trie-key trie-key
       :level (.getLevel k)
       :recency (.getRecency k)
       :part (some-> (.getPart k) (.toArray))
       :block-idx (.getBlockIndex k)})
    (catch IllegalArgumentException _)
    (catch IllegalStateException _)))

(defn parse-trie-file-path [^Path file-path]
  (-> (parse-trie-key (str (.getFileName file-path)))
      (assoc :file-path file-path)))

(defn table-name->table-path ^java.nio.file.Path [^String table-name]
  (Trie/getTablePath table-name))

(defn ->table-meta-dir ^java.nio.file.Path [table-name]
  (Trie/metaFileDir table-name))

(defrecord Segment [trie]
  ISegment
  (getTrie [_] trie)
  (getDataRel [this] (:data-rel this)))

(defn ->live-trie ^MemoryHashTrie [log-limit page-limit iid-rdr]
  (-> (doto (MemoryHashTrie/builder iid-rdr)
        (.setLogLimit log-limit)
        (.setPageLimit page-limit))
      (.build)))

(defn ->merge-task
  ([mp-pages] (->merge-task mp-pages (TemporalBounds.)))
  ([mp-pages ^TemporalBounds query-bounds]
   (let [leaves (ArrayList.)]
     (loop [[^MergePlanPage mp-page & more-mp-pages] mp-pages
            smallest-valid-from Long/MAX_VALUE
            largest-valid-to Long/MIN_VALUE
            smallest-system-from Long/MAX_VALUE
            non-taken-pages []]
       (if mp-page
         (let [page-temp-meta (.getTemporalMetadata mp-page)
               take-node? (and (Temporal/intersects page-temp-meta query-bounds)
                               (.testMetadata mp-page))]

           (if take-node?
             (do
               (.add leaves mp-page)
               (recur more-mp-pages
                      (min smallest-valid-from (.getMinValidFrom page-temp-meta))
                      (max largest-valid-to (.getMaxValidTo page-temp-meta))
                      (min smallest-system-from (.getMinSystemFrom page-temp-meta))
                      non-taken-pages))

             (recur more-mp-pages
                    smallest-valid-from
                    largest-valid-to
                    smallest-system-from
                    (cond-> non-taken-pages
                      (Temporal/intersectsSystemTime page-temp-meta query-bounds)
                      (conj mp-page)))))

         (when (seq leaves)
           (let [valid-time (TemporalDimension. smallest-valid-from largest-valid-to)]
             (loop [[^MergePlanPage page & more-pages] non-taken-pages]
               (when page
                 (let [page-temp-meta (.getTemporalMetadata page)
                       page-largest-system-from (.getMaxSystemFrom page-temp-meta)]
                   (when (and (<= smallest-system-from page-largest-system-from)
                              (.intersects (TemporalDimension. (.getMinValidFrom page-temp-meta)
                                                               (.getMaxValidTo page-temp-meta))
                                           valid-time))
                     (.add leaves page))
                   (recur more-pages)))))
           (vec leaves)))))))
