(ns dev
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as tn]
            [crux.embedded-kafka :as ek]
            [crux.bootstrap :as b]
            [crux.io :as cio])
  (:import [kafka.server KafkaServerStartable]
           [org.apache.zookeeper.server ServerCnxnFactory]
           [clojure.lang IDeref Var$Unbound]
           [java.io Closeable]
           [java.util.concurrent CancellationException]))

;; Inspired by
;; https://medium.com/@maciekszajna/reloaded-workflow-out-of-the-box-be6b5f38ea98

(def instance)
(def init)

(defn close-future [future]
  (try
    (future-cancel future)
    @future
    (catch CancellationException ignore)))

(defn start []
  (alter-var-root
   #'instance #(cond (not (bound? #'init))
                     (throw (IllegalStateException. "init not set."))

                     (or (nil? %)
                         (instance? Var$Unbound %)
                         (realized? %))
                     (future-call init)

                     :else
                     (throw (IllegalStateException. "Already running."))))
  :starting)

(defn stop []
  (when (and (bound? #'instance)
             (future? instance))
    (alter-var-root #'instance close-future))
  :stopped)

(defn reset []
  (stop)
  (tn/refresh :after 'dev/start))

(defn ^Closeable closeable
  ([value]
   (closeable value identity))
  ([value close]
   (reify
     IDeref
     (deref [_]
       value)
     Closeable
     (close [_]
       (close value)))))

(defn with-system-var [do-with-system target-var]
  (fn [system]
    (do (alter-var-root target-var (constantly system))
        (try
          (do-with-system system)
          (finally
            (alter-var-root target-var (constantly nil)))))))

(defn ^Closeable new-zk [{:keys [storage-dir]}]
  (closeable
    (ek/start-zookeeper
     (io/file storage-dir "zk-snapshot")
     (io/file storage-dir "zk-log"))
    (fn [^ServerCnxnFactory zk]
      (.shutdown zk))))

(defn ^Closeable new-kafka [{:keys [storage-dir]}]
  (closeable
   (ek/start-kafka-broker
    {"log.dir" (.getAbsolutePath (io/file storage-dir "kafka-log"))})
   (fn [^KafkaServerStartable kafka]
     (.shutdown kafka)
     (.awaitShutdown kafka))))

(defn ^Closeable new-kv-store [{:keys [storage-dir]
                                :as config}]
  (b/start-kv-store (assoc config :db-dir (io/file storage-dir "data"))))

(defn ^Closeable new-index-node [kv-store config]
  (closeable
   (future
     (b/start-system kv-store {}))
   (fn [crux]
     (close-future crux))))

(defn new-crux-system [config]
  (fn [do-with-system]
    (with-open [zk (new-zk config)
                kafka (new-kafka config)
                kv-store (new-kv-store config)
                index-node (new-index-node kv-store config)]
      (do-with-system (merge config {:zk @zk
                                     :kafka @kafka
                                     :kv-store kv-store
                                     :index-node @index-node})))))

(def config {:storage-dir "dev-storage"
             :kv-backend "rocksdb"})

(def with-crux-system
  (new-crux-system config))

(def system)

(alter-var-root
 #'init (constantly
         #(with-crux-system (-> (comp deref :index-node)
                                (with-system-var #'system)))))

(defn delete-storage []
  (stop)
  (cio/delete-dir (:storage-dir config))
  :ok)
