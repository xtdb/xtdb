(ns crux.bench
  (:require [crux.fixtures :refer [random-person start-system kv]]
            [crux.kv :as cr]))

(defn bench [& {:keys [n batch-size ts] :or {n 1000
                                             batch-size 10
                                             ts (java.util.Date.)}}]
  (start-system
   (fn []
     (time
      (doseq [[i people] (map-indexed vector (partition-all batch-size (take n (repeatedly random-person))))]
        (cr/-put kv people ts))))))
