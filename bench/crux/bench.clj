(ns crux.bench
  (:require [crux.fixtures :as f :refer [random-person start-system *kv*]]
            [crux.kv :as cr]
            [crux.query :as q]
            [crux.core :refer [db]]
            [crux.memdb])
  (:import [java.util Date]))

(defn bench [& {:keys [n batch-size ts queries kv] :or {n 1000
                                                        batch-size 10
                                                        queries 100
                                                        ts (Date.)
                                                        kv :rocks}}]

  ((case kv
     :rocks f/with-rocksdb
     :lmdb f/with-lmdb
     :mem f/with-memdb)
   (fn []
     (start-system
      (fn []
        ;; Insert data
        (time
         (doseq [[i people] (map-indexed vector (partition-all batch-size (take n (repeatedly random-person))))]
           (cr/-put *kv* people ts)))

        ;; Basic query
        (time
         (doseq [i (range queries)]
           (q/q (db *kv*) {:find ['e]
                           :where [['e :name "Ivan"]]}))))))))

;; Datomic: 100 queries against 1000 dataset = 40-50 millis
