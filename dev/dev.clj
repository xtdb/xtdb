(ns dev
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as tn]
            [crux.bootstrap :as b]
            [crux.byte-utils :as bu]
            [crux.doc :as doc]
            [crux.embedded-kafka :as ek]
            [crux.http-server :as srv]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kafka :as k]
            [crux.rdf :as rdf]
            [sys :refer [start stop clear reset]])
  (:import [kafka.server KafkaServerStartable]
           [org.apache.zookeeper.server ServerCnxnFactory]
           [ch.qos.logback.classic Level Logger]
           [org.slf4j LoggerFactory]
           [java.io Closeable]))

(def storage-dir "dev-storage")
(def options {:db-dir (str storage-dir "/data")
              :zookeeper-data-dir (str storage-dir "/zookeeper")
              :kafka-log-dir (str storage-dir "/kafka-log")})
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

(defn start-crux-dev-system
  [with-system-fn]
  (let [options (merge b/default-options options)]
    (with-open [zookeeper (start-zookeeper options)
                kafka (start-kafka options)]
      (b/start-system
        options
        (fn [system]
          (with-system-fn (merge system {:zookeeper zookeeper
                                         :kafka kafka})))))))

(defn make-crux-system-init-fn []
  ((sys/make-init-fn #'system (fn [cont] (start-crux-dev-system cont)))))

(alter-var-root #'sys/init (constantly make-crux-system-init-fn))

(defn delete-storage []
  (stop)
  (cio/delete-dir storage-dir)
  :ok)

(defn set-log-level! [ns level]
  (.setLevel ^Logger (LoggerFactory/getLogger (name ns))
             (Level/valueOf (name level))))

(comment
  (start)

  (set-log-level! 'crux.rdf :debug)
  ;; Download from http://wiki.dbpedia.org/services-resources/ontology
  (with-open [in (io/input-stream (io/file "../dbpedia/mappingbased_properties_en.nt"))]
    (rdf/submit-ntriples (k/->KafkaTxLog (:kafka-producer system)
                                         (get-in system [:options :tx-topic])
                                         (get-in system [:options :doc-topic]))
                         in
                         1000))

  (doc/q (doc/db (:kv-store system))
         '{:find [e]
           :where [[e :http://xmlns.com/foaf/0.1/name "Aristotle"]]}))
