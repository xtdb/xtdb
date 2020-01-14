(ns crux.bench.utils
  (:require [clojure.data.json]))

(defn output [mp]
  (println (json/write-str mp)))
