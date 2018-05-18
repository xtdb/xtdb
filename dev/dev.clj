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

(defn ^Closeable new-crux [{:keys [storage-dir]}]
  (closeable
   (let [kv-promise (promise)]
     {:kv kv-promise
      :crux (future
              (with-redefs [b/start-kv-store
                            (let [f b/start-kv-store]
                              (fn [& args]
                                (let [kv (apply f args)]
                                  (deliver kv-promise kv)
                                  kv)))]
                (b/start-system
                 {:db-dir (io/file storage-dir "data")})))})
   (fn [{:keys [crux]}]
     (close-future crux))))

(defn new-crux-system [config]
  (fn [do-with-system]
    (with-open [zk (new-zk config)
                kafka (new-kafka config)
                crux (new-crux config)]
      (do-with-system {:zk @zk
                       :kafka @kafka
                       :crux (:crux @crux)
                       :kv @(:kv @crux)}))))

(def config {:storage-dir "dev-storage"})

(def with-crux-system
  (new-crux-system config))

(defn delete-storage []
  (stop)
  (cio/delete-dir (:storage-dir config))
  :ok)

(def system)

(alter-var-root
 #'init (constantly
         #(with-crux-system (-> (comp deref :crux)
                                (with-system-var #'system)))))
