(ns xtdb.arrow-edn-test
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.test :as t]
            [xtdb.arrow :as arrow]
            xtdb.mirrors.time-literals
            [xtdb.serde :as serde]
            xtdb.serde.types
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.nio.file FileVisitOption Files Path)
           (xtdb.arrow Relation)))

(def ^:dynamic *regen?*
  #_{:clj-kondo/ignore [:single-logical-operand]}
  (or false
      #_true ;; <<no-commit>>
      ))

(defn read-arrow-edn-file [path-ish]
  (serde/->clj-types (read-string (slurp (.toFile (util/->path path-ish))))))

(defn ->arrow-edn [^Relation rel]
  {:schema (.getSchema rel), :data (serde/->clj-types (.getAsMaps rel))})

(defn- write-arrow-edn-file! [^Path path, data]
  (with-open [out (io/writer (.toFile path))]
    (pp/pprint data out)))

#_{:clojure-lsp/ignore [:clojure-lsp/unused-public-var]}
(defn wrap-regen [f]
  (binding [*regen?* true]
    (f)))

(defn maybe-write-arrow-edn! [arrow-edn expected-path-ish]
  (let [expected-path (util/->path expected-path-ish)]

    (when (or *regen?* (not (util/path-exists expected-path)))
      (write-arrow-edn-file! expected-path arrow-edn))))

(defn- add-edn-ext ^Path [^Path path]
  (.resolve (.getParent path) (format "%s.edn" (.getFileName path))))

(defn- with-arrow-edn-pairs [expected-path-ish actual-path-ish file-pattern f]
  (let [expected-dir (util/->path expected-path-ish)
        actual-dir (util/->path actual-path-ish)]
    (when *regen?*
      (let [file (.toFile expected-dir)]
        (when (.isDirectory file)
          (util/delete-dir expected-dir)
          (.mkdirs file))))

    (doseq [^Path path (iterator-seq (.iterator (Files/walk actual-dir (make-array FileVisitOption 0))))
            :let [file-name (str (.getFileName path))]
            :when (and (str/ends-with? file-name ".arrow")
                       (or (nil? file-pattern)
                           (re-matches file-pattern file-name)))]
      (arrow/with-arrow-file tu/*allocator* path
        (fn [res]
          (write-arrow-edn-file! (add-edn-ext path) res)

          (when *regen?*
            (write-arrow-edn-file! (doto (-> expected-dir
                                             (.resolve (.relativize actual-dir path))
                                             add-edn-ext)
                                     (-> (.getParent) (util/mkdirs)))
                                   res)))))

    (doseq [^Path expected-path (iterator-seq (.iterator (Files/walk expected-dir (make-array FileVisitOption 0))))
            :let [actual (-> actual-dir
                             (.resolve (.relativize expected-dir expected-path)))
                  file-name (str (.getFileName expected-path))]
            :when (.endsWith file-name ".arrow.edn")]
      (f (.relativize expected-dir expected-path)
         (serde/->clj-types (read-arrow-edn-file expected-path))
         (serde/->clj-types (read-arrow-edn-file actual))))))

(defmacro check-arrow-edn-dir
  ([expected-path-ish actual-path-ish]
   `(check-arrow-edn-dir ~expected-path-ish ~actual-path-ish nil))
  ([expected-path-ish actual-path-ish file-pattern]
   `(let [expected-dir# (util/->path ~expected-path-ish)]
      (#'with-arrow-edn-pairs expected-dir# ~actual-path-ish ~file-pattern
        (fn [expected-path# expected# actual#]
          (t/is (= expected# actual#)
                (str expected-path#)))))))
