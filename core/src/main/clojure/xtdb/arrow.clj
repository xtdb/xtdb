(ns xtdb.arrow
  "Utilities for working with Arrow things"
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            xtdb.mirrors.time-literals
            [xtdb.serde :as serde]
            xtdb.serde.types
            [xtdb.util :as util])
  (:import (java.nio.file Files LinkOption Path)
           [java.util Spliterators]
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           (xtdb.arrow Relation)))

(defn read-arrow-stream-file
  ([path-ish]
   (with-open [al (RootAllocator.)]
     (read-arrow-stream-file al path-ish)))

  ([^BufferAllocator al, path-ish]
   (with-open [r (Relation/openFromArrowStream al (Files/readAllBytes (util/->path path-ish)))]
     {:schema (.getSchema r)
      :batch (.getAsMaps r)})))

(defn with-arrow-file
  ([path-ish f]
   (with-open [al (RootAllocator.)]
     (with-arrow-file al path-ish f)))

  ([^BufferAllocator al, path-ish f]
   (with-open [loader (Relation/loader al (util/->path path-ish))
               cursor (.openCursor loader al)]
     (f {:schema (.getSchema loader)
         :batches (->> (iterator-seq (Spliterators/iterator cursor))
                       (map (fn [^Relation rel]
                              (vec (.getAsMaps rel)))))}))))

(defn read-arrow-file
  ([al path-ish]
   (with-arrow-file al path-ish
     (fn [res]
       (update res :batches vec))))

  ([path-ish]
   (with-arrow-file path-ish
     (fn [res]
       (update res :batches vec)))))

(defn read-arrow-edn-file [path-ish]
  (serde/->clj-types (read-string (slurp (.toFile (util/->path path-ish))))))

(defn- write-expected-file [^Path path, data]
  (with-open [out (io/writer (.toFile path))]
    (pp/pprint data out)))

(defn ->arrow-edn [^Relation rel]
  {:schema (.getSchema rel), :data (serde/->clj-types (.getAsMaps rel))})

(defn maybe-write-arrow-edn! [arrow-edn expected-path-ish]
  (let [expected-path (util/->path expected-path-ish)]

    #_{:clj-kondo/ignore [:single-logical-operand]}
    (when (or (not (util/path-exists expected-path))
              ;; uncomment to reset the expected files
              #_true ;; <<no-commit>>
              )
      (write-expected-file expected-path arrow-edn) ; <<no-commit>>
      )))
