(ns crux-node-only-system
  (:require [clojure.tools.logging :as log]
            [crux.bootstrap.cluster-node :as cluster-node]
            [crux.http-server :as srv]
            [crux.kafka.embedded :as ek]
            [crux.db :as db]
            [crux.query :as q]
            [integrant.core :as ig]
            [crux.kafka :as k]))

(def tx-topic-config
  {"retention.ms" (str Long/MAX_VALUE)})

(def config {:crux/cluster-node {:db-dir "dev-storage/data"
                                 :bootstrap-servers "localhost:9092"}})

(defmethod ig/init-key :crux/cluster-node [_ opts]
  (cluster-node/start-cluster-node opts))

(defmethod ig/halt-key! :crux/cluster-node [_ ^java.io.Closeable closeable]
  (.close closeable))

(declare system)

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
    (:tx-log (:crux/cluster-node system))
    [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
      {:crux.db/id :http://dbpedia.org/resource/Pablo_Picasso
       :name "Pablo"
       :last-name "Picasso"}
      #inst "2018-05-18T09:20:27.966-00:00"]])

  (q/q (q/db (:kv-store (:crux/cluster-node system)))
       '{:find [e]
         :where [[e :name "Pablo"]]}))
