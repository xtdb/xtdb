(ns crux.rocksdb
  (:require [byte-streams :as bs])
  (:import [org.rocksdb ReadOptions Slice]))

(defn get [db k & [pred]]
  (let [i (.newIterator db)]
    (try
      (.seek i k)
      (when (and (.isValid i) (if pred (pred i) (bs/bytes= (.key i) k)))
        [(.key i) (.value i)])
      (finally
        (.close i)))))

(defn rocks-iterator->seq [i]
  (lazy-seq
   (when (and (.isValid i))
     (cons (conj [(.key i) (.value i)])
           (do (.next i)
               (rocks-iterator->seq i))))))

(defn seek-and-iterate
  "TODO, improve by getting prefix-same-as-start to work, so we don't
  need an upper-bound."
  [db k upper-bound]
  (let [read-options (ReadOptions.)
        i (.newIterator db (.setIterateUpperBound read-options (Slice. upper-bound)))]
    (try
      (.seek i k)
      (doall (rocks-iterator->seq i))
      (finally
        (.close i)))))
