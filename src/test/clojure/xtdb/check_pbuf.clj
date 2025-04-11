(ns xtdb.check-pbuf
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.test :as t]
            [xtdb.block-catalog :as block-cat]
            [xtdb.table-catalog :as table-cat]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.util :as util])
  (:import [java.nio.file CopyOption FileVisitOption Files Path StandardCopyOption]
           org.roaringbitmap.buffer.ImmutableRoaringBitmap
           (xtdb.block.proto Block TableBlock)
           (xtdb.log.proto TrieDetails)
           (xtdb.util HyperLogLog)))

(defn multi-parse-fn
  "Tries to parse a byte array from a given list of parse-fns. Throws if non parses."
  [parse-fns]
  (fn [v]
    (or (some #(% v) parse-fns)
        (throw (RuntimeException. "No matching parse-fn!")))))

(defmacro ignoring-ex [& body]
  `(try
     ~@body
     (catch Exception _e#)))

#_{:clj-kondo/ignore [:unused-private-var]}
(defn- delete-and-recreate-dir [^Path path]
  (when (.. path toFile isDirectory)
    (util/delete-dir path)
    (.mkdirs (.toFile path))))

(defn- <-Block [^Block block]
  (-> (block-cat/<-Block block)
      (pr-str) (read-string)))

(defn- <-TableBlock [^TableBlock block]
  (-> (table-cat/<-table-block block)
      (update :hlls update-vals (juxt hash HyperLogLog/estimate))
      (update :tries (partial mapv
                              (fn [^TrieDetails trie-details]
                                {:trie-key (.getTrieKey trie-details)
                                 :data-file-size (.getDataFileSize trie-details)
                                 :trie-metadata (some-> (trie-cat/<-trie-metadata (.getTrieMetadata trie-details))
                                                        (update :iid-bloom (juxt hash #(.getCardinality ^ImmutableRoaringBitmap %))))})))))

(defn- write-pbuf-edn-file [^Path path parse-fn]
  (let [edn-file (.resolveSibling path (str (.getFileName path) ".edn"))]
    (with-open [out (io/writer (.toFile edn-file))]
      (pp/pprint (parse-fn (Files/readAllBytes path)) out))

    edn-file))

#_{:clj-kondo/ignore [:unused-private-var]}
(defn- copy-expected-file [^Path file, ^Path expected-dir, ^Path actual-dir]
  (Files/copy file (doto (.resolve expected-dir (.relativize actual-dir file))
                     (-> (.getParent) (util/mkdirs)))
              ^"[Ljava.nio.file.CopyOption;" (into-array CopyOption #{StandardCopyOption/REPLACE_EXISTING})))

(defn check-pbuf
  ([expected-dir actual-dir] (check-pbuf expected-dir actual-dir {}))

  ([^Path expected-dir, ^Path actual-dir,
    {:keys [parse-fn file-pattern]
     :or {parse-fn (multi-parse-fn [#(some-> (ignoring-ex (Block/parseFrom ^bytes %)) <-Block)
                                    #(some-> (ignoring-ex (TableBlock/parseFrom ^bytes %)) <-TableBlock)])}}]

   ;; uncomment if you want to remove files
   #_(delete-and-recreate-dir expected-dir) ;; <<no-commit>>
   (doseq [^Path path (iterator-seq (.iterator (Files/walk actual-dir (make-array FileVisitOption 0))))
           :let [file-name (str (.getFileName path))]
           :when (and (str/ends-with? file-name ".binpb")
                      (or (nil? file-pattern)
                          (re-matches file-pattern file-name)))]
     (doto (write-pbuf-edn-file path parse-fn)
       ;; uncomment this to reset the expected file (but don't commit it)
       #_(copy-expected-file expected-dir actual-dir))) ;; <<no-commit>>

   (doseq [^Path expected (iterator-seq (.iterator (Files/walk expected-dir (make-array FileVisitOption 0))))
           :let [file-name (str (.getFileName expected))]
           :when (and (.endsWith file-name ".binpb.edn")
                      (or (nil? file-pattern)
                          (re-matches file-pattern file-name)))
           :let [file-key (.relativize expected-dir expected)
                 actual (.resolve actual-dir file-key)]]
     (t/testing (str file-key)
       (t/is (= (read-string (slurp (.toFile expected)))
                (read-string (slurp (.toFile actual)))))))))
