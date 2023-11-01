(ns xtdb.file-list
  (:import [java.nio.file Path]
           [java.util NavigableSet]))

(defn list-files-under-prefix [^NavigableSet file-name-cache ^Path prefix]
  (let [prefix-depth (.getNameCount prefix)]
    (->> (.tailSet ^NavigableSet file-name-cache prefix)
         (take-while #(.startsWith ^Path % prefix))
         (keep (fn [^Path path]
                 (when (> (.getNameCount path) prefix-depth)
                   (.subpath path 0 (inc prefix-depth)))))
         (distinct))))
