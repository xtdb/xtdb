(ns example-system
  (:require [clojure.tools.logging :as log]
            [crux.api :as api]
            [crux.http-server :as srv]
            [crux.kafka.embedded :as ek]
            [integrant.core :as ig]))

(def config {:crux/embedded-kafka {:crux.kafka.embedded/zookeeper-data-dir "dev-storage/zookeeper"
                                   :crux.kafka.embedded/kafka-log-dir "dev-storage/kafka-log"
                                   :crux.kafka.embedded/kafka-port 9092}
             :crux/local-node {:db-dir "dev-storage/data"
                               :bootstrap-servers "localhost:9092"
                               :embedded-kafka (ig/ref :crux/embedded-kafka)}
             :crux/http-server {:local-node (ig/ref :crux/local-node)}})

(defmethod ig/init-key :crux/local-node [_ opts]
  (api/start-local-node opts))

(defmethod ig/halt-key! :crux/local-node [_ ^java.io.Closeable embedded-kafka]
  (.close embedded-kafka))

(defmethod ig/init-key :crux/embedded-kafka [_ opts]
  (ek/start-embedded-kafka opts))

(defmethod ig/halt-key! :crux/embedded-kafka [_ ^java.io.Closeable closeable]
  (.close closeable))

(defmethod ig/init-key :crux/http-server [_ {:keys [local-node port] :as opts}]
  (log/infof "Firing up http %s" opts)
  (srv/start-http-server local-node opts))

(defmethod ig/halt-key! :crux/http-server [_ ^java.io.Closeable server]
  (.close server))

(def system nil)

(defn start-system []
  (alter-var-root #'system (fn [_] (ig/init config)))
  :started)

(defn stop-system []
  (when (and (bound? #'system)
             (not (nil? system)))
    (alter-var-root #'system (fn [system] (ig/halt! system)))
    :stopped))
