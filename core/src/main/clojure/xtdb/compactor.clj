(ns xtdb.compactor
  (:require [integrant.core :as ig]
            [xtdb.compactor.job-calculator :as jc]
            [xtdb.db-catalog :as db]
            [xtdb.util :as util])
  (:import xtdb.api.CompactorConfig
           (xtdb.compactor Compactor Compactor$Driver Compactor$Impl)
           (xtdb.database Database$Mode)))

(def ^:dynamic *ignore-signal-block?* false)
(def ^:dynamic *recency-partition* nil)

(defmethod ig/expand-key :xtdb/compactor [k ^CompactorConfig config]
  {k {:threads (.getThreads config)
      :metrics-registry (ig/ref :xtdb.metrics/registry)}})

(def ^:dynamic *page-size* 1024)

(defn- open-compactor [{:keys [metrics-registry threads]}]
  (Compactor$Impl. (Compactor$Driver/real metrics-registry *page-size* *recency-partition*)
                   metrics-registry
                   (jc/->JobCalculator)
                   *ignore-signal-block?* threads))

(defmethod ig/init-key :xtdb/compactor [_ {:keys [threads] :as opts}]
  (if (pos? threads)
    (open-compactor opts)
    Compactor/NOOP))

(defmethod ig/halt-key! :xtdb/compactor [_ compactor]
  (util/close compactor))

(defmethod ig/expand-key ::for-db [k {:keys [base mode]}]
  {k {:base base
      :mode mode
      :allocator (ig/ref :xtdb.db-catalog/allocator)
      :storage (ig/ref :xtdb.db-catalog/storage)
      :state (ig/ref :xtdb.db-catalog/state)}})

(defmethod ig/init-key ::for-db [_ {{:keys [^Compactor compactor]} :base, :keys [allocator storage state ^Database$Mode mode]}]
  (if (= mode Database$Mode/READ_ONLY)
    (.openForDatabase Compactor/NOOP allocator storage state)
    (.openForDatabase compactor allocator storage state)))

(defmethod ig/halt-key! ::for-db [_ compactor-for-db]
  (util/close compactor-for-db))

(defn compact-all!
  "`timeout` is now required, explicitly specify `nil` if you want to wait indefinitely."
  [node timeout]

  (-> (.getCompactor (db/primary-db node))
      (.compactAll timeout)))
