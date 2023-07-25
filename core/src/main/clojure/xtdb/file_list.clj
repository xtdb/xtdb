(ns xtdb.file-list
  (:require [clojure.string :as string])
  (:import java.util.NavigableSet))

(defn list-files-under-prefix [^NavigableSet file-name-cache prefix]
  (->> (.tailSet ^NavigableSet file-name-cache prefix)
       (take-while #(string/starts-with? % prefix))
       (into [])))
