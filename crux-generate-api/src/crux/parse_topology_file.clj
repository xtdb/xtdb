(ns crux.parse-topology-file
  (:require [clojure.java.io :as io]
            [clojure.string :as string]))

(defn parse-namespace [ns-string]
  (last (string/split ns-string #" ")))

(defn parse-topology-file [filename]
  (prn (slurp filename)))