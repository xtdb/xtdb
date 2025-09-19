(ns xtdb.arrow
  "Utilities for working with Arrow things"
  (:require xtdb.mirrors.time-literals
            xtdb.serde
            xtdb.serde.types
            [xtdb.util :as util])
  (:import (java.nio.file Files)
           [java.util Spliterators]
           (org.apache.arrow.memory RootAllocator)
           (xtdb.arrow Relation)))

(defn read-arrow-stream-file [path-ish]
  (with-open [al (RootAllocator.)
              r (Relation/openFromArrowStream al (Files/readAllBytes (util/->path path-ish)))]
    {:schema (.getSchema r)
     :batch (.getAsMaps r)}))

(defn with-arrow-file [path-ish f]
  (with-open [al (RootAllocator.)
              loader (Relation/loader al (util/->path path-ish))
              cursor (.openCursor loader al)]
    (f {:schema (.getSchema loader)
        :batches (->> (iterator-seq (Spliterators/iterator cursor))
                      (map (fn [^Relation rel]
                             (vec (.getAsMaps rel)))))})))

(defn read-arrow-file [path-ish]
  (with-arrow-file path-ish
    (fn [res]
      (update res :batches vec))))
