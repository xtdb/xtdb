(ns dev
  "Internal development namespace for Crux. For end-user usage, see
  examples.clj"
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as tn]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s]
            [crux.api :as api]
            [crux.bootstrap :as b]
            [crux.byte-utils :as bu]
            [crux.db :as db]
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
  (:import [crux.api Crux LocalNode ICruxSystem]
           [ch.qos.logback.classic Level Logger]
           org.slf4j.LoggerFactory
           java.io.Closeable
           java.util.Date))

(def storage-dir "dev-storage")

(defn dev-option-defaults [storage-dir]
  {:crux.kafka.embedded/zookeeper-data-dir (str storage-dir "/zookeeper")
   :crux.kafka.embedded/kafka-log-dir (str storage-dir "/kafka-log")
   :crux.kafka.embedded/kafka-port 9092
   :dev/embed-kafka? true
   :dev/http-server? true
   :dev/system-start-fn api/start-local-node
   :db-dir (str storage-dir "/data")
   :bootstrap-servers "localhost:9092"
   :server-port 3000})

(def dev-options (dev-option-defaults storage-dir))

(def ^ICruxSystem system)

(defn start-dev-system ^crux.api.LocalNode [{:dev/keys [embed-kafka? http-server? system-start-fn] :as options}]
  (let [started (atom [])]
    (try
      (let [embedded-kafka (when embed-kafka?
                             (doto (ek/start-embedded-kafka options)
                               (->> (swap! started conj))))
            local-node (doto (system-start-fn options)
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

;; Example to use a standalone system with the normal Crux dev
;; workflow:

;; (ns dev)
;; (def storage-dir "dev-storage-standalone")
;; (def dev-options (merge (dev-option-defaults storage-dir)
;;                         {:event-log-dir (str storage-dir "/event-log")
;;                          :crux.tx/event-log-sync-interval-ms 1000
;;                          :dev/embed-kafka? false
;;                          :dev/http-server? false
;;                          :dev/system-start-fn api/start-standalone-system}))

(when (io/resource (str (System/getenv "USER") ".clj"))
  (load (System/getenv "USER")))
