(ns xtdb.check-pbuf
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.test :as t]
            [xtdb.arrow-edn-test :as aet]
            [xtdb.block-catalog :as block-cat]
            [xtdb.pbuf :as pbuf]
            [xtdb.util :as util])
  (:import [java.nio.file CopyOption FileVisitOption Files Path StandardCopyOption]
           (xtdb.block.proto Block TableBlock)))

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

(defn- write-pbuf-edn-file [^Path path parse-fn update-fn]
  (let [edn-file (.resolveSibling path (str (.getFileName path) ".edn"))]
    (with-open [out (io/writer (.toFile edn-file))]
      (pp/pprint (update-fn (parse-fn path (Files/readAllBytes path))) out))

    edn-file))

#_{:clj-kondo/ignore [:unused-private-var]}
(defn- copy-expected-file [^Path file, ^Path expected-dir, ^Path actual-dir]
  (Files/copy file (doto (.resolve expected-dir (.relativize actual-dir file))
                     (-> (.getParent) (util/mkdirs)))
              ^"[Ljava.nio.file.CopyOption;" (into-array CopyOption #{StandardCopyOption/REPLACE_EXISTING})))

(defn check-pbuf
  ([expected-dir actual-dir] (check-pbuf expected-dir actual-dir {}))

  ([^Path expected-dir, ^Path actual-dir,
    {:keys [parse-fn file-pattern update-fn]
     :or {parse-fn (fn [path ^bytes ba]
                     (cond
                       (re-matches #".*/tables/.*" (str path))
                       (some-> (ignoring-ex (TableBlock/parseFrom ba)) pbuf/<-TableBlock)

                       :else
                       (some-> (ignoring-ex (Block/parseFrom ^bytes ba)) <-Block)))
          update-fn identity}}]

   (when aet/*regen?*
     (delete-and-recreate-dir expected-dir))

   (doseq [^Path path (iterator-seq (.iterator (Files/walk actual-dir (make-array FileVisitOption 0))))
           :let [file-name (str (.getFileName path))]
           :when (and (str/ends-with? file-name ".binpb")
                      (or (nil? file-pattern)
                          (re-matches file-pattern file-name)))]
     (doto (write-pbuf-edn-file path parse-fn update-fn)
       (cond-> aet/*regen?* (copy-expected-file expected-dir actual-dir))))

   (doseq [^Path expected (iterator-seq (.iterator (Files/walk expected-dir (make-array FileVisitOption 0))))
           :let [file-name (str (.getFileName expected))]
           :when (and (.endsWith file-name ".binpb.edn")
                      (or (nil? file-pattern)
                          (re-matches file-pattern file-name)))
           :let [file-key (.relativize expected-dir expected)
                 actual (.resolve actual-dir file-key)]]
     (t/testing (str file-key)
       (t/is (= (read-string (slurp (.toFile expected)))
                (update-fn (read-string (slurp (.toFile actual))))))))))
