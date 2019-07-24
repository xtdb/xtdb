(ns crux-node-embedded-kafka
  (:require [clojure.tools.logging :as log]
            [crux.http-server :as srv]
            [crux.kafka.embedded :as ek]
            [crux.db :as db]
            [crux.bootstrap :as b]
            [crux.query :as q]
            [integrant.core :as ig]
            [crux.kafka :as k]))

(def tx-topic-config
  {"retention.ms" (str Long/MAX_VALUE)})

(def config {:crux/cluster-node {:db-dir "dev-storage/data"
                                 :bootstrap-servers "localhost:9092"}})

(defmethod ig/init-key :crux/cluster-node [_ opts]
  (b/start-node k/node-config opts))

(defmethod ig/halt-key! :crux/cluster-node [_ ^java.io.Closeable closeable]
  (.close closeable))

(declare node)

(defn start-node []
  (alter-var-root #'node (fn [_] (ig/init config)))
  :started)

(defn stop-node []
  (when (and (bound? #'node)
             (not (nil? node)))
    (alter-var-root #'node (fn [node] (ig/halt! node)))
    :stopped))

(comment
  (start-node)

  (db/submit-tx
    (:tx-log (:crux/cluster-node node))
    [[:crux.tx/put
      {:crux.db/id :dbpedia.resource/Pablo-Picasso
       :name "Pablo"
       :last-name "Picasso"}
      #inst "2018-05-18T09:20:27.966-00:00"]])

  (q/q (q/db (:kv-store (:crux/cluster-node node)))
       '{:find [e]
         :where [[e :name "Pablo"]]}))
