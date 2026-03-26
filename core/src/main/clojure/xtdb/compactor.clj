(ns xtdb.compactor
  (:require [integrant.core :as ig]
            [xtdb.compactor.job-calculator :as jc]
            [xtdb.db-catalog :as db]
            [xtdb.util :as util])
  (:import xtdb.api.CompactorConfig
           (xtdb.compactor Compactor Compactor$Driver Compactor$Impl)
           xtdb.NodeBase))

(def ^:dynamic *ignore-signal-block?* false)
(def ^:dynamic *recency-partition* nil)

(defmethod ig/expand-key :xtdb/compactor [k ^CompactorConfig config]
  {k {:threads (.getThreads config)
      :base (ig/ref :xtdb/base)}})

(def ^:dynamic *page-size* 1024)

(defn- open-compactor [{:keys [^NodeBase base threads]}]
  (Compactor$Impl. (Compactor$Driver/real *page-size* *recency-partition*)
                   (.getMeterRegistry base)
                   (jc/->JobCalculator)
                   *ignore-signal-block?* threads))

(defmethod ig/init-key :xtdb/compactor [_ {:keys [threads] :as opts}]
  (if (pos? threads)
    (open-compactor opts)
    Compactor/NOOP))

(defmethod ig/halt-key! :xtdb/compactor [_ compactor]
  (util/close compactor))

(defn compact-all!
  "`timeout` is now required, explicitly specify `nil` if you want to wait indefinitely."
  [node timeout]

  (-> (.getCompactor (db/primary-db node))
      (.compactAllSync timeout)))
