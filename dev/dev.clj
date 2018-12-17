(ns dev
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as tn]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [crux.api :as api]
            [crux.bootstrap :as b]
            [crux.byte-utils :as bu]
            [crux.index :as idx]
            [crux.kafka.embedded :as ek]
            [crux.http-server :as srv]
            [crux.codec :as c]
            [crux.io :as cio]
            [crux.kafka :as k]
            [crux.rdf :as rdf])
  (:import [crux.api LocalNode ICruxSystem]
           [ch.qos.logback.classic Level Logger]
           org.slf4j.LoggerFactory
           java.io.Closeable))

; tag::dev-options[]
(def storage-dir "dev-storage")
(def dev-options {:crux.kafka.embedded/zookeeper-data-dir (str storage-dir "/zookeeper")
                  :crux.kafka.embedded/kafka-log-dir (str storage-dir "/kafka-log")
                  :crux.kafka.embedded/kafka-port 9092
                  :dev/embed-kafka? true
                  :dev/http-server? true
                  :db-dir (str storage-dir "/data")
                  :bootstrap-servers "localhost:9092"
                  :server-port 3000})
; end::dev-options[]

(def ^ICruxSystem system)

; tag::dev-system[]
(defn start-dev-system ^crux.api.LocalNode [{:dev/keys [embed-kafka? http-server?] :as options}]
  (let [started (atom [])] ;;<1>
    (try
      (let [embedded-kafka (when embed-kafka? ;;<2>
                             (doto (ek/start-embedded-kafka options)
                               (->> (swap! started conj))))
            local-node (doto (api/start-local-node options)
                         (->> (swap! started conj)))
            http-server (when http-server?
                          (srv/start-http-server local-node options))]
        (assoc local-node
               :http-server http-server
               :embedded-kafka embedded-kafka))
      (catch Throwable t
        (doseq [c (reverse @started)]
          (cio/try-close c))
        (throw t)))))
; end::dev-system[]

(defn stop-dev-system ^crux.api.LocalNode [{:keys [http-server embedded-kafka] :as system}]
  (doseq [c [http-server system embedded-kafka]]
    (cio/try-close c)))

(defn start []
  (alter-var-root #'system (fn [_] (start-dev-system dev-options)))
  :started)

(defn stop []
  (when (and (bound? #'system)
             (not (nil? system)))
    (alter-var-root #'system stop-dev-system))
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
