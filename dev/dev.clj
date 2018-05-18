(ns dev
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as tn]
            [crux.embedded-kafka :as ek]
            [crux.bootstrap :as b]
            [crux.io :as cio])
  (:import [kafka.server KafkaServerStartable]
           [org.apache.zookeeper.server ServerCnxnFactory]
           [clojure.lang IDeref]
           [java.io Closeable]
           [java.util.concurrent CancellationException]))

;; Inspired by
;; https://medium.com/@maciekszajna/reloaded-workflow-out-of-the-box-be6b5f38ea98

(defonce instance (atom nil))
(def init (atom #(throw (IllegalStateException. "init not set."))))

(defn close-future [future]
  (future-cancel future)
  (try
    @future
    (catch CancellationException ignore)))

(defn start []
  (swap! instance #(if (or (nil? %) (realized? %))
                     (future-call @init)
                     (throw (IllegalStateException. "Already running."))))
  :starting)

(defn stop []
  (some-> instance deref close-future)
  :stopped)

(defn reset []
  (stop)
  (tn/refresh :after 'dev/start))

(defn publishing-state [do-with-system target-atom]
  #(do (reset! target-atom %)
       (try
         (do-with-system %)
         (finally
           (reset! target-atom nil)))))

(defn ^Closeable closeable
  ([value]
   (closeable value identity))
  ([value close]
   (reify
     IDeref
     (deref [_] value)
     Closeable
     (close [_] (close value)))))

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

(defonce system (atom nil))
(reset! init #(with-crux-system (-> (comp deref :crux)
                                    (publishing-state system))))

(defn delete-storage []
  (stop)
  (cio/delete-dir (:storage-dir config))
  :ok)
