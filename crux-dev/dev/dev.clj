(ns dev
  "Internal development namespace for Crux. For end-user usage, see
  examples.clj"
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as tn]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [crux.node :as n]
            [crux.standalone :as standalone]
            [crux.db :as db]
            [crux.api :as crux]
            [crux.index :as idx]
            [crux.kafka.embedded :as ek]
            [crux.kv :as kv]
            [crux.http-server :as srv]
            [crux.codec :as c]
            [crux.io :as cio]
            [crux.kafka :as k]
            [crux.memory :as mem]
            [crux.rdf :as rdf]
            [crux.query :as q])
  (:import [crux.api Crux ICruxAPI]
           [ch.qos.logback.classic Level Logger]
           org.slf4j.LoggerFactory
           java.io.Closeable
           java.util.Date))

(def storage-dir "dev-storage")

(defn dev-option-defaults [storage-dir]
  {:crux.node/topology k/topology
   :crux.kafka.embedded/zookeeper-data-dir (str storage-dir "/zookeeper")
   :crux.kafka.embedded/kafka-log-dir (str storage-dir "/kafka-log")
   :crux.kafka.embedded/kafka-port 9092
   :dev/embed-kafka? true
   :dev/http-server? true
   :dev/node-start-fn n/start
   :crux.node/db-dir (str storage-dir "/data")
   :crux.kafka/bootstrap-servers "localhost:9092"
   :server-port 3000})

(def dev-options (dev-option-defaults storage-dir))

(def ^ICruxAPI node)

(defn start-dev-node ^crux.api.ICruxAPI [{:dev/keys [embed-kafka? http-server? node-topology node-start-fn] :as options}]
  (let [started (atom [])]
    (try
      (let [embedded-kafka (when embed-kafka?
                             (doto (ek/start-embedded-kafka options)
                               (->> (swap! started conj))))
            cluster-node (doto (node-start-fn options)
                           (->> (swap! started conj)))
            http-server (when http-server?
                          (srv/start-http-server (merge cluster-node options)))]
        (assoc cluster-node
               :http-server http-server
               :embedded-kafka embedded-kafka))
      (catch Throwable t
        (doseq [c (reverse @started)]
          (cio/try-close c))
        (throw t)))))

(defn stop-dev-node ^crux.api.ICruxAPI [{:keys [http-server embedded-kafka] :as node}]
  (doseq [c [http-server node embedded-kafka]]
    (cio/try-close c)))

(defn start []
  (alter-var-root #'node (fn [_] (start-dev-node dev-options)))
  :started)

(defn stop []
  (when (and (bound? #'node)
             (not (nil? node)))
    (alter-var-root #'node stop-dev-node))
  :stopped)

(defn clear []
  (alter-var-root #'node (constantly nil)))

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
             (when level
               (Level/valueOf (name level)))))

(defn get-log-level! [ns]
  (some->> (.getLevel ^Logger (LoggerFactory/getLogger (name ns)))
           (str)
           (.toLowerCase)
           (keyword)))

(defmacro with-log-level [ns level & body]
  `(let [level# (get-log-level! ~ns)]
     (try
       (set-log-level! ~ns ~level)
       ~@body
       (finally
         (set-log-level! ~ns level#)))))

(n/install-uncaught-exception-handler!)

;; Usage, create a dev/$USER.clj file like this, and add it to
;; .gitignore:

;; Optional, helps when evaluating buffer in Emacs, will be evaluated
;; in the context of the dev ns:
;; (ns dev)
;; ;; Override the storage dir:
;; (def storage-dir "foo")
;; ;; And override some options:
;; (def dev-options (merge (dev-option-defaults storage-dir)
;;                         {:server-port 9090}))

;; Example to use a standalone node with the normal Crux dev
;; workflow:

;; (ns dev)
;; (def storage-dir "dev-storage-standalone")
;; (def dev-options (merge (dev-option-defaults storage-dir)
;;                         {:crux.node/topology :crux.standalone/topology
;;                          :event-log-dir (str storage-dir "/event-log")
;;                          :crux.standalone/event-log-sync-interval-ms 1000
;;                          :dev/embed-kafka? false
;;                          :dev/http-server? false
;;                          :dev/node-start-fn standalone/start-node}))

(when (io/resource (str (System/getenv "USER") ".clj"))
  (load (System/getenv "USER")))
