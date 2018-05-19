(ns dev
  (:require [clojure.java.io :as io]
            [crux.embedded-kafka :as ek]
            [crux.kafka :as k]
            [crux.bootstrap :as b]
            [crux.io :as cio]
            [sys :refer [start stop clear reset]])
  (:import [kafka.server KafkaServerStartable]
           [org.apache.zookeeper.server ServerCnxnFactory]
           [java.io Closeable]))

(defn ^Closeable start-zk [{:keys [storage-dir]}]
  (sys/closeable
   (ek/start-zookeeper
    (io/file storage-dir "zk-snapshot")
    (io/file storage-dir "zk-log"))
   (fn [^ServerCnxnFactory zk]
     (.shutdown zk))))

(defn ^Closeable start-kafka [{:keys [storage-dir]}]
  (sys/closeable
   (ek/start-kafka-broker
    {"log.dir" (.getAbsolutePath (io/file storage-dir "kafka-log"))})
   (fn [^KafkaServerStartable kafka]
     (.shutdown kafka)
     (.awaitShutdown kafka))))

(defn ^Closeable start-crux-system [with-system-fn {:keys [storage-dir
                                                           bootstrap-servers
                                                           group-id]
                                                    :as options}]
  (sys/closeable-future-call
   #(with-open [zk (start-zk options)
                kafka (start-kafka options)
                kv-store (b/start-kv-store (assoc options :db-dir (io/file storage-dir "data")))
                kafka-consumer (k/create-consumer {"bootstrap.servers" bootstrap-servers
                                                   "group.id" group-id})
                kafka-admin-client (k/create-admin-client {"bootstrap.servers" bootstrap-servers})]
      (->> {:zk @zk
            :kafka @kafka
            :kv-store kv-store
            :kafka-consumer kafka-consumer
            :kafka-admin-client kafka-admin-client}
           (merge options)
           (with-system-fn)))))

(defn start-index-node [{:keys [kv-store kafka-consumer kafka-admin-client]
                         :as options}]
  (b/start-system kv-store kafka-consumer kafka-admin-client options))

(def config {:storage-dir "dev-storage"
             :kv-backend "rocksdb"
             :bootstrap-servers ek/*kafka-bootstrap-servers*
             :group-id "0"})
(def system)

(alter-var-root
 #'sys/init (constantly (sys/make-init-fn #(start-crux-system % config)
                                          start-index-node
                                          #'system)))

(defn delete-storage []
  (stop)
  (cio/delete-dir (:storage-dir config))
  :ok)
