(ns crux.parse-topology-file
  (:require [clojure.java.io :as io]
            [clojure.string :as string]))

(defn get-namespace [file-contents]
  (second (first file-contents)))

(defn filter-first [pred val]
  (first (filter pred val)))

(defn get-topology-map [file-contents]
  (->> file-contents
       (filter-first (fn [xs] (some #(= 'topology %) xs)))
       (filter-first seq?)
       (filter-first map?)))

(defn parse-topology-file [filename]
  (let [file-contents (read-string (str "[" (slurp filename) "]"))
        node-ns (get-namespace file-contents)
        topology-map (get-topology-map file-contents)]
    topology-map))
