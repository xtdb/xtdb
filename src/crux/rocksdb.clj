(ns crux.rocksdb
  (:require [byte-streams :as bs]))

(defn get [db k]
  (let [i (.newIterator db)]
    (try
      (.seek i k)
      (when (and (.isValid i) (bs/bytes= (.key i) k))
        [(.key i) (.value i)])
      (finally
        (.close i)))))

(defn rocks-iterator->seq [i pred?]
  (lazy-seq
   (when (and (.isValid i) (or (not pred?) (pred? i)))
     (cons (conj [(.key i) (.value i)])
           (do (.next i)
               (rocks-iterator->seq i pred?))))))


(defn seek-and-iterate [db k]
  (let [i (.newIterator db)]
    (try
      (.seek i k)
      (doall (rocks-iterator->seq i (fn [i]
                                      (bs/bytes= k (byte-array (take (count k) (.key i)))))))
      (finally
        (.close i)))))
