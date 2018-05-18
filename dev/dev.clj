(ns dev
  (:require [crux.embedded-kafka :as ek]
            [crux.bootstrap :as b]
            [crux.io :as cio]
            [clojure.tools.namespace.repl :as tn])
  (:import [kafka.server KafkaServerStartable]
           [org.apache.zookeeper.server ServerCnxnFactory]
           [java.util.concurrent Future]))

(def zk nil)
(def kafka nil)
(def kv nil)
(def system nil)

(def zk-snapshot-dir "zk-snapshot")
(def zk-log-dir "zk-log")
(def kafka-log-dir "kafka-log")
(def db-dir "data")

(defn start-zk []
  (alter-var-root #'zk (some-fn identity (fn [_] (ek/start-zookeeper zk-snapshot-dir zk-log-dir))))
  :started)

(defn start-kafka []
  (alter-var-root #'kafka (some-fn identity (fn [_] (ek/start-kafka-broker {"log.dir" kafka-log-dir}))))
  :started)

(defn stop-zk []
  (alter-var-root #'zk
                  (fn [^ServerCnxnFactory zk-server]
                    (some-> zk-server .shutdown)))
  :stopped)

(defn stop-kafka []
  (alter-var-root #'kafka
                  (fn [^KafkaServerStartable kafka]
                    (some-> kafka .shutdown)))
  :stopped)

(defn start-system []
  (start-zk)
  (start-kafka)
  (alter-var-root #'system
                  (some-fn identity
                           (fn [_]
                             (future
                               (with-redefs [b/start-kv-store
                                             (let [f b/start-kv-store]
                                               (fn [& args]
                                                 (let [kv (apply f args)]
                                                   (alter-var-root #'kv (constantly kv))
                                                   kv)))]
                                 (b/start-system {:db-dir db-dir}))))))
  :started)

(defn stop-system []
  (alter-var-root #'system
                  (fn [system]
                    (some-> system future-cancel)
                    nil))
  (alter-var-root #'kv (constantly nil))
  (stop-kafka)
  (stop-zk)
  :stopped)

(defn delete-storage []
  (stop-system)
  (doseq [dir [zk-snapshot-dir zk-log-dir kafka-log-dir db-dir]]
    (cio/delete-dir dir))
  :ok)
