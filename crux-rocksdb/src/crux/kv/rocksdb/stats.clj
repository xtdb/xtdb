(ns crux.kv.rocksdb.stats
  (:require [clojure.string :as string]
            [crux.metrics.dropwizard :as dropwizard]))

(defn file-size [s]
  (let [[size unit] (-> s
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
  [s]
  (-> s
      (string/split #"Sum")
      (second)
      (string/split #"\s+")
      (nth 12)
      (read-string)))

(defn write-speed
  "MB/s"
  [s]
  (-> s
      (string/split #"Sum")
      (second)
      (string/split #"\s+")
      (nth 13)
      (read-string)))

(defn total-keys-in [s]
  (-> s
      (string/split #"Sum")
      (second)
      (string/split #"\s+")
      (nth 18)
      (read-string)))

(defn total-keys-dropped [s]
  (-> s
      (string/split #"Sum")
      (second)
      (string/split #"\s+")
      (nth 19)
      (read-string)))

(defn db-uptime
  "Secs"
  [s]
  (-> s
      (string/split #"Uptime")
      (second)
      (string/split #"\s+")
      (nth 3)
      (read-string)))

(defn cumulative-writes
"Secs"
  [s]
  (-> s
      (string/split #"Cumulative writes: ")
      (second)
      (string/split #"\s+")
      (first)
      (read-string)))

(def gauges {:write-speed #'crux.kv.rocksdb.stats/write-speed
             :cumulative-writes #'crux.kv.rocksdb.stats/cumulative-writes
             :read-speed #'crux.kv.rocksdb.stats/read-speed
             :db-uptime #'crux.kv.rocksdb.stats/db-uptime
             :file-size #'crux.kv.rocksdb.stats/file-size
             :total-keys-dropped #'crux.kv.rocksdb.stats/total-keys-dropped
             :total-keys-in #'crux.kv.rocksdb.stats/total-keys-in})

(defn assign-gauges [{:keys [crux.metrics/registry crux.kv.rockdb/kv]}]
  (run! (fn [[k v]] (dropwizard/gauge registry ["rocksdb" (name k)] #(v (:db kv)))) gauges))
