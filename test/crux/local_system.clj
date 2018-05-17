(ns crux.local-system
  (:require [crux.embedded-kafka :as ek]
            [crux.bootstrap :as b]
            [crux.kv-store :as kv-store]
            [crux.io :as cio])
  (:import [kafka.server KafkaServerStartable]
           [org.apache.zookeeper.server ServerCnxnFactory]
           [java.util.concurrent Future]))

(declare ^ServerCnxnFactory local-zk-server
         ^KafkaServerStartable local-kafka-server
         ^Future local-system
         ^kv-store/CruxKvStore local-kv)

(defn start-local-kafka []
  (def local-zk-server (ek/start-zookeeper "zk-snapshot" "zk-log"))
  (def local-kafka-server (ek/start-kafka-broker {"log.dir" "kafka-log"})))

(defn stop-local-kafka []
  (.shutdown local-kafka-server)
  (.shutdown local-zk-server))

(defn start-local-system []
  (start-local-kafka)
  (def local-system (future (b/start-system {:db-dir "data"})))
  (Thread/sleep 1000)
  (def local-kv b/kv))

(defn stop-local-system []
  (future-cancel local-system)
  (stop-local-kafka))

(defn clear-local-system []
  (doseq [dir ["zk-snapshot" "zk-log" "kafka-log" "data"]]
    (cio/delete-dir dir)))
