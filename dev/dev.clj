(ns dev
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as tn]
            [clojure.tools.logging :as log]
            [crux.api :as api]
            [crux.bootstrap :as b]
            [crux.byte-utils :as bu]
            [crux.doc :as doc]
            [crux.kafka.embedded :as ek]
            [crux.http-server :as srv]
            [crux.codec :as c]
            [crux.io :as cio]
            [crux.kafka :as k]
            [crux.rdf :as rdf])
  (:import crux.api.LocalNode
           [ch.qos.logback.classic Level Logger]
           org.slf4j.LoggerFactory
           java.io.Closeable))

(def storage-dir "dev-storage")
(def dev-options {:db-dir (str storage-dir "/data")
                  :zookeeper-data-dir (str storage-dir "/zookeeper")
                  :kafka-log-dir (str storage-dir "/kafka-log")
                  :embed-kafka? true
                  :kafka-port 9092
                  :bootstrap-servers "localhost:9092"
                  :http-server? true
                  :server-port 3000})

(def system)

(defn ^LocalNode start-dev-system [{:keys [embed-kafka? http-server?] :as options}]
  (let [started (atom [])]
    (try
      (let [embedded-kafka (when embed-kafka?
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

(defn ^LocalNode stop-dev-system [{:keys [http-server embedded-kafka] :as system}]
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
