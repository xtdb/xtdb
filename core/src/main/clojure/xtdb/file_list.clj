(ns xtdb.file-list
  (:require [clojure.java.io :as io])
  (:import [java.nio.file Path]
           [java.util NavigableSet]))

(defn file-name->path [^String file-name]
  (.toPath (io/file file-name)))

(defn add-filename [^NavigableSet file-name-cache ^String file-name]
  (.add file-name-cache file-name))

(defn add-filename-list [^NavigableSet file-name-cache file-name-list]
  (.addAll file-name-cache (mapv file-name->path file-name-list)))

(defn remove-filename [^NavigableSet file-name-cache ^String file-name]
  (.remove file-name-cache (file-name->path file-name)))

(defn list-files [^NavigableSet file-name-cache]
  (mapv str file-name-cache))

(defn list-files-under-prefix [^NavigableSet file-name-cache prefix]
  (let [prefix-path ^Path (file-name->path prefix)
        prefix-depth (.getNameCount prefix-path)]
    (->> (.tailSet ^NavigableSet file-name-cache prefix-path)
         (take-while #(.startsWith ^Path % prefix-path))
         (keep (fn [^Path path]
                 (when (> (.getNameCount path) prefix-depth)
                   (str (.subpath path 0 (inc prefix-depth))))))
         (distinct))))
