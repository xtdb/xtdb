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

(def storage-dir "dev-storage")
(def options {:db-dir (io/file storage-dir "data")
              :zookeeper-data-dir (io/file storage-dir "zookeeper")
              :kafka-log-dir (io/file storage-dir "kafka-log")})
(def system)

(defn ^Closeable start-zookeeper [{:keys [zookeeper-data-dir]}]
  (sys/closeable
   (ek/start-zookeeper (io/file zookeeper-data-dir))
   (fn [^ServerCnxnFactory zk]
     (.shutdown zk))))

(defn ^Closeable start-kafka [{:keys [kafka-log-dir]}]
  (sys/closeable
   (ek/start-kafka-broker
    {"log.dir" (str (io/file kafka-log-dir))})
   (fn [^KafkaServerStartable kafka]
     (.shutdown kafka)
     (.awaitShutdown kafka))))

(defn with-crux-system [do-with-system-fn {:keys [bootstrap-servers
                                                  group-id]
                                           :as options}]
  (with-open [zookeeper (start-zookeeper options)
              kafka (start-kafka options)
              kv-store (b/start-kv-store options)
              kafka-producer (k/create-producer {"bootstrap.servers" bootstrap-servers})
              kafka-consumer (k/create-consumer {"bootstrap.servers" bootstrap-servers
                                                 "group.id" group-id})
              kafka-admin-client (k/create-admin-client {"bootstrap.servers" bootstrap-servers
                                                         "request.timeout.ms" "5000"})]
    (->> {:zookeeper @zookeeper
          :kafka @kafka
          :kv-store kv-store
          :kafka-producer kafka-producer
          :kafka-consumer kafka-consumer
          :kafka-admin-client kafka-admin-client
          :options options}
         (do-with-system-fn))))

(defn start-index-node [running? {:keys [kv-store kafka-consumer kafka-admin-client options]
                                  :as system}]
  (b/start-system kv-store kafka-consumer kafka-admin-client running? options))

(defn make-crux-system-init-fn []
  (fn []
    (let [running? (atom true)
          init-fn (sys/make-init-fn
                   #(-> (sys/with-system-var % #'system)
                        (with-crux-system (merge b/default-options options)))
                   (partial start-index-node running?)
                   (fn [_]
                     (reset! running? false)))]
      (init-fn))))

(alter-var-root #'sys/init (constantly (make-crux-system-init-fn)))

(defn delete-storage []
  (stop)
  (cio/delete-dir storage-dir)
  :ok)

(defn set-log-level! [ns level]
  (.setLevel ^Logger (LoggerFactory/getLogger (name ns))
             (Level/valueOf (name level))))

(comment
  (start)

  ;; Download from http://wiki.dbpedia.org/services-resources/ontology
  (with-open [in (io/input-stream (io/file "../dbpedia/mappingbased_properties_en.nt"))]
    (k/transact-ntriples-ops (:kafka-producer system) in (:tx-topic system) (:doc-topic system) 1000))

  (q/q (crux/db (:kv-store system))
       '{:find [iri]
         :where [[e :http://xmlns.com/foaf/0.1/name "Aristotle"]
                 [e :crux.rdf/iri iri]]}))
