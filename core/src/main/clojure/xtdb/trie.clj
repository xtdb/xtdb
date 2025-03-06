(ns xtdb.trie
  (:require [xtdb.buffer-pool]
            [xtdb.util :as util])
  (:import com.carrotsearch.hppc.ByteArrayList
           (java.nio.file Path)
           java.time.LocalDate
           (java.util ArrayList)
           xtdb.BufferPool
           (xtdb.trie ISegment MemoryHashTrie Trie Trie$Key)
           (xtdb.util TemporalBounds TemporalDimension)
           (xtdb.log.proto TrieDetails TrieMetadata)))

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

(defprotocol MergePlanPage
  (load-page [mpg ^BufferPool buffer-pool vsr-cache])
  (test-metadata [msg])
  (temporal-bounds [msg]))

(defn ->live-trie ^MemoryHashTrie [log-limit page-limit iid-rdr]
  (-> (doto (MemoryHashTrie/builder iid-rdr)
        (.setLogLimit log-limit)
        (.setPageLimit page-limit))
      (.build)))

(defn max-valid-to ^long [^TemporalBounds tb]
  (.getUpper (.getValidTime tb)))

(defn min-valid-from ^long [^TemporalBounds tb]
  (.getLower (.getValidTime tb)))

(defn min-system-from ^long [^TemporalBounds tb]
  (.getLower (.getSystemTime tb)))

(defn max-system-from ^long [^TemporalBounds tb]
  (.getMaxSystemFrom tb))

(defn ->merge-task
  ([mp-pages] (->merge-task mp-pages (TemporalBounds.)))
  ([mp-pages ^TemporalBounds query-bounds]
   (let [leaves (ArrayList.)]
     (loop [[mp-page & more-mp-pages] mp-pages
            node-taken? false
            smallest-valid-from Long/MAX_VALUE
            largest-valid-to Long/MIN_VALUE
            smallest-system-from Long/MAX_VALUE
            non-taken-pages []]
       (if mp-page
         (let [^TemporalBounds page-bounds (temporal-bounds mp-page)
               take-node? (and (.intersects page-bounds query-bounds)
                               (test-metadata mp-page))]

           (if take-node?
             (do
               (.add leaves mp-page)
               (recur more-mp-pages
                      true
                      (min smallest-valid-from (min-valid-from page-bounds))
                      (max largest-valid-to (max-valid-to page-bounds))
                      (min smallest-system-from (min-system-from page-bounds))
                      non-taken-pages))

             (recur more-mp-pages
                    node-taken?
                    smallest-valid-from
                    largest-valid-to
                    smallest-system-from
                    (cond-> non-taken-pages
                      (.intersects (.getSystemTime page-bounds) (.getSystemTime query-bounds))
                      (conj mp-page)))))

         (when node-taken?
           (let [valid-time (TemporalDimension. smallest-valid-from largest-valid-to)]
             (loop [[page & more-pages] non-taken-pages]
               (when page
                 (let [page-valid-time (.getValidTime ^TemporalBounds (temporal-bounds page))
                       page-largest-system-from (max-system-from (temporal-bounds page))]
                   (when (and (<= smallest-system-from page-largest-system-from)
                              (.intersects valid-time page-valid-time))
                     (.add leaves page))
                   (recur more-pages)))))
           (vec leaves)))))))
