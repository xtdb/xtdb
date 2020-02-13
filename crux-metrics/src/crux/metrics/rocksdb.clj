(ns crux.metrics.rocksdb
  (:require [clojure.string :as string]
            [crux.metrics.dropwizard :as dropwizard]))

(defn file-size [kv]
  (let [[size unit] (-> (.getProperty kv "rocksdb.stats")
                        (string/split #"Sum")
                        (second)
                        (string/split #"\s+")
                        (->> (partition 2))
                        (second))]
    (* (read-string size)
       (case unit
         "KB" 1000
         "MB" 1000000
         "TB" 1000000000))))

(defn read-speed
  "MB/s"
  [kv]
  (-> (.getProperty kv "rocksdb.stats")
      (string/split #"Sum")
      (second)
      (string/split #"\s+")
      (nth 12)
      (read-string)))

(defn write-speed
  "MB/s"
  [kv]
  (-> (.getProperty kv "rocksdb.stats")
      (string/split #"Sum")
      (second)
      (string/split #"\s+")
      (nth 13)
      (read-string)))

(defn total-keys-in [kv]
  (-> (.getProperty kv "rocksdb.stats")
      (string/split #"Sum")
      (second)
      (string/split #"\s+")
      (nth 18)
      (read-string)))

(defn total-keys-dropped [kv]
  (-> (.getProperty kv "rocksdb.stats")
      (string/split #"Sum")
      (second)
      (string/split #"\s+")
      (nth 19)
      (read-string)))

(defn db-uptime
  "Secs"
  [kv]
  (-> (.getProperty kv "rocksdb.stats")
      (string/split #"Uptime")
      (second)
      (string/split #"\s+")
      (nth 3)
      (read-string)))

(defn cumulative-writes
"Secs"
  [kv]
  (-> (.getProperty kv "rocksdb.stats")
      (string/split #"Cumulative writes: ")
      (second)
      (string/split #"\s+")
      (first)
      (read-string)))

(defn num-snapshots
  [kv]
  (read-string (.getProperty kv "rocksdb.num-snapshots")))

(def gauges {:write-speed #'crux.metrics.rocksdb/write-speed
             :cumulative-writes #'crux.metrics.rocksdb/cumulative-writes
             :read-speed #'crux.metrics.rocksdb/read-speed
             :db-uptime #'crux.metrics.rocksdb/db-uptime
             :file-size #'crux.metrics.rocksdb/file-size
             :total-keys-dropped #'crux.metrics.rocksdb/total-keys-dropped
             :total-keys-in #'crux.metrics.rocksdb/total-keys-in
             :num-snapshots #'crux.metrics.rocksdb/num-snapshots})

(defn assign-gauges [registry {:keys [crux.node/kv-store]}]
  (run! (fn [[k v]]
          (dropwizard/gauge registry ["rocksdb" (name k)] #(v (:db (:kv kv-store))))) gauges))
