(ns dev
  (:require [clojure.java.io :as io]
            [crux.embedded-kafka :as ek]
            [crux.kafka :as k]
            [crux.bootstrap :as b]
            [crux.io :as cio]
            [crux.core :as crux]
            [crux.query :as q]
            [sys :refer [start stop clear reset]])
  (:import [kafka.server KafkaServerStartable]
           [org.apache.zookeeper.server ServerCnxnFactory]
           [ch.qos.logback.classic Level Logger]
           [org.slf4j LoggerFactory]
           [java.io Closeable]))

(def config {:storage-dir "dev-storage"
             :db-dir "dev-storage/data"
             :replication-factor 1})
(def system)

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

(defn with-crux-system [do-with-system-fn {:keys [bootstrap-servers
                                                  group-id]
                                           :as options}]
  (with-open [zk (start-zk options)
              kafka (start-kafka options)
              kv-store (b/start-kv-store options)
              kafka-producer (k/create-producer {"bootstrap.servers" bootstrap-servers})
              kafka-consumer (k/create-consumer {"bootstrap.servers" bootstrap-servers
                                                 "group.id" group-id})
              kafka-admin-client (k/create-admin-client {"bootstrap.servers" bootstrap-servers
                                                         "request.timeout.ms" "5000"})]
    (->> {:zk @zk
          :kafka @kafka
          :kv-store kv-store
          :kafka-producer kafka-producer
          :kafka-consumer kafka-consumer
          :kafka-admin-client kafka-admin-client}
         (merge options)
         (do-with-system-fn))))

(defn start-index-node [{:keys [kv-store kafka-consumer kafka-admin-client]
                         :as options}]
  (b/start-system kv-store kafka-consumer kafka-admin-client options))

(defn make-crux-system-init-fn []
  (fn []
    (let [running? (atom true)
          init-fn (sys/make-init-fn
                   #(-> (sys/with-system-var % #'system)
                        (with-crux-system
                          (merge b/default-options config {:running? running?})))
                   start-index-node
                   (fn [_]
                     (reset! running? false)))]
      (init-fn))))

(alter-var-root #'sys/init (constantly (make-crux-system-init-fn)))

(defn delete-storage []
  (stop)
  (cio/delete-dir (:storage-dir config))
  :ok)

(defn set-log-level! [ns level]
  (.setLevel ^Logger (LoggerFactory/getLogger (name ns))
             (Level/valueOf (name level))))

(comment
  (start)

  ;; Download from http://wiki.dbpedia.org/services-resources/ontology
  (with-open [in (io/input-stream (io/file "../dbpedia/mappingbased_properties_en.nt"))]
    (k/transact-ntriples (:kafka-producer system) in (:topic system) 1000))

  (q/q (crux/db (:kv-store system))
       '{:find [iri]
         :where [[e :http://xmlns.com/foaf/0.1/name "Aristotle"]
                 [e :crux.rdf/iri iri]]}))
