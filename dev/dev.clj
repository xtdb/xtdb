(ns dev
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as tn]
            [crux.embedded-kafka :as ek]
            [crux.bootstrap :as b]
            [crux.io :as cio])
  (:import [kafka.server KafkaServerStartable]
           [org.apache.zookeeper.server ServerCnxnFactory]
           [java.util.concurrent Future]))

(def zk nil)
(def kafka nil)
(def kv nil)
(def crux nil)

(def storage-dir "dev-storage")

(defn start-zk []
  (alter-var-root #'zk (some-fn identity
                                (fn [_]
                                  (ek/start-zookeeper (io/file storage-dir "zk-snapshot")
                                                      (io/file storage-dir "zk-log")))))
  :started)

(defn stop-zk []
  (alter-var-root #'zk
                  (fn [^ServerCnxnFactory zk-server]
                    (some-> zk-server .shutdown)))
  :stopped)

(defn start-kafka []
  (alter-var-root #'kafka
                  (some-fn identity
                           (fn [_]
                             (ek/start-kafka-broker {"log.dir"
                                                     (.getAbsolutePath (io/file storage-dir "kafka-log"))}))))
  :started)

(defn stop-kafka []
  (alter-var-root #'kafka
                  (fn [^KafkaServerStartable kafka]
                    (some-> kafka .shutdown)
                    (some-> kafka .awaitShutdown)))
  :stopped)

(defn start-crux []
  (alter-var-root #'crux
                  (some-fn identity
                           (fn [_]
                             (future
                               (with-redefs [b/start-kv-store
                                             (let [f b/start-kv-store]
                                               (fn [& args]
                                                 (let [kv (apply f args)]
                                                   (alter-var-root #'kv (constantly kv))
                                                   kv)))]
                                 (b/start-system {:db-dir (io/file storage-dir "data")})))))))

(defn stop-crux []
  (alter-var-root #'crux
                  (fn [system]
                    (some-> system future-cancel)
                    nil))
  (alter-var-root #'kv (constantly nil)))

(defn start-system []
  (start-zk)
  (start-kafka)
  (start-crux)
  :started)

(defn stop-system []
  (stop-crux)
  (stop-kafka)
  (stop-zk)
  :stopped)

(defn delete-storage []
  (stop-system)
  (cio/delete-dir storage-dir)
  :ok)
