(ns crux-node-only-system
  (:require [clojure.tools.logging :as log]
            [crux.api :as api]
            [crux.http-server :as srv]
            [crux.kafka.embedded :as ek]
            [integrant.core :as ig]))

 ;; (def s (api/start-local-node {:db-dir "dev-storage/data"
 ;;                              :bootstrap-servers "localhost:9092"}))

(def config {:crux/local-node {:db-dir "dev-storage/data"
                               :bootstrap-servers "localhost:9092"}})

(defmethod ig/init-key :crux/local-node [_ opts]
  (api/start-local-node opts))

(defmethod ig/halt-key! :crux/local-node [_ ^java.io.Closeable closeable]
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

(comment
  (start-system)

  (db/submit-tx
   (:tx-log system)
   [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
     {:crux.db/id :http://dbpedia.org/resource/Pablo_Picasso
      :name "Pablo"
      :last-name "Picasso"}
     #inst "2018-05-18T09:20:27.966-00:00"]]))
