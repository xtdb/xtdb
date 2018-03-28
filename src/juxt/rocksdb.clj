(ns juxt.rocksdb
  (:require [byte-streams :as bs]))

(defn get [db k]
  (let [i (.newIterator db)]
    (try
      (.seek i k)
      (when (and (.isValid i) (bs/bytes= (.key i) k))
        [(.key i) (.value i)])
      (finally
        (.close i)))))
