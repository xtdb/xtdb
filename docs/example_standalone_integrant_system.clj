(ns example-standalone-integrant-system
  (:require [crux.api :as api]
            [integrant.core :as ig]))

(def config {:crux/standalone {:kv-backend "crux.kv.memdb.MemKv"
                               :db-dir "data/db-dir-1"}})

(defmethod ig/init-key :crux/standalone [_ opts]
  (api/start-standalone-system opts))

(defmethod ig/halt-key! :crux/standalone [_ ^java.io.Closeable closeable]
  (.close closeable))

(def system nil)

(defn start-system []
  (alter-var-root #'system (fn [_] (ig/init config)))
  :started)

(defn stop-system []
  (when (and (bound? #'system)
             (not (nil? system)))
    (alter-var-root #'system (fn [system] (ig/halt! system)))
    :stopped))
