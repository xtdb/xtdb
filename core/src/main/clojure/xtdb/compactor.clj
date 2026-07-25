(ns xtdb.compactor
  (:require [xtdb.compactor.job-calculator :as jc]
            [xtdb.db-catalog :as db])
  (:import (xtdb.compactor Compactor Compactor$Driver Compactor$Factory Compactor$Impl)))

(def ^:dynamic *ignore-signal-block?* false)
(def ^:dynamic *recency-partition* nil)

(def ^:dynamic *page-size* 1024)

(defn ->factory ^xtdb.compactor.Compactor$Factory []
  (reify Compactor$Factory
    (create [_ meter-registry threads]
      (if (pos? threads)
        (Compactor$Impl. (Compactor$Driver/real *page-size* *recency-partition*)
                         meter-registry
                         (jc/->JobCalculator)
                         *ignore-signal-block?* threads)
        Compactor/NOOP))))

(defn compact-all!
  "Compacts until idle. `timeout` is required; pass `nil` to wait indefinitely for
   pending work. A failed compaction job is surfaced (thrown) rather than waited on,
   so this won't hang (or time out) on a job that can never complete."
  [node timeout]

  (-> (.getCompactor (db/primary-db node))
      (.compactAllSync timeout)))
