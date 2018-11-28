(ns dev
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as tn]
            [crux.api :as api]
            [crux.bootstrap :as b]
            [crux.byte-utils :as bu]
            [crux.doc :as doc]
            [crux.embedded-kafka :as ek]
            [crux.http-server :as srv]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kafka :as k]
            [crux.rdf :as rdf])
  (:import kafka.server.KafkaServerStartable
           org.apache.zookeeper.server.ServerCnxnFactory
           [ch.qos.logback.classic Level Logger]
           org.slf4j.LoggerFactory
           java.io.Closeable))

(def storage-dir "dev-storage")
(def dev-options {:db-dir (str storage-dir "/data")
                  :zookeeper-data-dir (str storage-dir "/zookeeper")
                  :kafka-log-dir (str storage-dir "/kafka-log")})

(def system)

(defn start []
  (alter-var-root
   #'system (constantly
             (let [{:keys [zookeeper-data-dir kafka-log-dir]
                    :as options} (merge b/default-options dev-options)
                   zookeeper (ek/start-zookeeper (io/file zookeeper-data-dir))
                   kafka (ek/start-kafka-broker {"log.dir" (str (io/file kafka-log-dir))})]
               (merge (api/start-local-node options) {:zookeeper zookeeper
                                                      :kafka kafka}))))
  :started)

(defn stop []
  (when (and (bound? #'system)
             (not (nil? system)))
    (alter-var-root #'system
                    (fn [{:keys [kafka zookeeper] :as node}]
                      (.close ^Closeable node)
                      (ek/stop-kafka-broker kafka)
                      (ek/stop-zookeeper zookeeper))))
  :stopped)

(defn clear []
  (alter-var-root #'system (constantly nil)))

(defn reset []
  (stop)
  (let [result (tn/refresh :after 'dev/start)]
    (if (instance? Throwable result)
      (throw result)
      result)))

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
