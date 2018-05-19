(ns dev
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as tn]
            [crux.embedded-kafka :as ek]
            [crux.kafka :as k]
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

(defn ^Closeable closeable [value close-fn]
  (reify
    IDeref
    (deref [_]
      value)
    Closeable
    (close [_]
      (close-fn value))))

(defn ^Closeable closeable-future-call [f]
  (closeable
   (future-call f)
   (fn [this]
     (try
       (when-not (future-cancelled? this)
         (future-cancel this)
         @this)
       (catch CancellationException ignore)))))

(defn start []
  (alter-var-root
   #'instance (fn [instance]
                (cond
                  (not (bound? #'init))
                  (throw (IllegalStateException. "init not set."))

                  (or (nil? instance)
                      (instance? Var$Unbound instance))
                  (cast Closeable (init))

                  :else
                  (throw (IllegalStateException. "Already running.")))))
  :started)

(defn stop []
  (when (and (bound? #'instance)
             (not (nil? instance)))
    (alter-var-root #'instance #(.close ^Closeable %)))
  :stopped)

(defn clear []
  (alter-var-root #'instance (constantly nil)))

(defn reset []
  (stop)
  (let [result (tn/refresh :after 'dev/start)]
    (if (instance? Throwable result)
      (throw result)
      result)))

(defn with-system-var [with-system-fn target-var]
  (fn [system]
    (try
      (alter-var-root target-var (constantly system))
      (with-system-fn system)
      (finally
        (alter-var-root target-var (constantly nil))))))

(defn with-system-promise [with-system-fn promise]
  (fn [system]
    (deliver promise system)
    (with-system-fn system)))

(defn make-init-fn [new-system-fn with-system-fn system-var]
  (fn []
    (let [started? (promise)
          instance (-> with-system-fn
                       (with-system-promise started?)
                       (with-system-var system-var)
                       (new-system-fn))]
      (while (not (or (deref @instance 100 false)
                      (deref started? 100 false))))
      instance)))

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

(defn ^Closeable new-crux-system [with-system-fn {:keys [storage-dir
                                                         bootstrap-servers
                                                         group-id]
                                                  :as options}]
  (closeable-future-call
   #(with-open [zk (new-zk options)
                kafka (new-kafka options)
                kv-store (b/start-kv-store (assoc options :db-dir (io/file storage-dir "data")))
                kafka-consumer (k/create-consumer {"bootstrap.servers" bootstrap-servers
                                                   "group.id" group-id})
                kafka-admin-client (k/create-admin-client {"bootstrap.servers" bootstrap-servers})]
      (->> {:zk @zk
            :kafka @kafka
            :kv-store kv-store
            :kafka-consumer kafka-consumer
            :kafka-admin-client kafka-admin-client}
           (merge options)
           (with-system-fn)))))

(defn start-index-node [{:keys [kv-store kafka-consumer kafka-admin-client]
                         :as options}]
  (b/start-system kv-store kafka-consumer kafka-admin-client options))

(def config {:storage-dir "dev-storage"
             :kv-backend "rocksdb"
             :bootstrap-servers ek/*kafka-bootstrap-servers*
             :group-id "0"})
(def system)

(alter-var-root
 #'init (constantly (make-init-fn #(new-crux-system % config)
                                  start-index-node
                                  #'system)))

(defn delete-storage []
  (stop)
  (cio/delete-dir (:storage-dir config))
  :ok)
