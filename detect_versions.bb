(require '[clojure.java.io :as io]
         '[clojure.pprint :refer [pprint]])

(def version-regexp #"<version>(.*)-(.*)</version>")

(defn extract-version
  [file-path]
  (->> file-path
       slurp
       (re-matcher version-regexp)
       re-find
       (drop 1)))

(defn group-by-version
  [base-dir]
  (filter #(some? (first %))
          (group-by
           #(-> % second first)
           (into {}
                 (for [f (filter
                          #(re-matches #"README.*" (.getName %))
                          (file-seq (io/file base-dir)))]
                   [(.getPath f) (extract-version f)])))))


(pprint
 (group-by-version (first *command-line-args*)))
