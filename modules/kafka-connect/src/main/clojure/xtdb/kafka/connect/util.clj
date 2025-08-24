(ns xtdb.kafka.connect.util
  (:import (org.apache.kafka.connect.data Struct)
           (org.apache.kafka.connect.connector ConnectRecord)
           (org.apache.kafka.connect.sink SinkRecord)))

(defn clone-connect-record [^ConnectRecord record {:keys [topic
                                                          kafka-partition
                                                          key-schema
                                                          key
                                                          value-schema
                                                          value
                                                          timestamp
                                                          headers]
                                                   :as _changes}]
  (.newRecord record
              (or topic (.topic record))
              (or kafka-partition (.kafkaPartition record))
              (or key-schema (.keySchema record))
              (or key (.key record))
              (or value-schema (.valueSchema record))
              (or value (.value record))
              (or timestamp (.timestamp record))
              (or headers (.headers record))))

(defn ->sink-record [{:keys [topic partition
                             key-schema key-value
                             value-schema value-value
                             offset]
                      :or {partition 0 offset 0}}]
  (SinkRecord. topic partition
    key-schema key-value
    value-schema value-value
    offset))

(defn struct-put-all [s m]
  (reduce
    (fn [^Struct s [k v]]
      (.put s (name k) v))
    s
    m))

(defn ->struct [schema m]
  (-> (Struct. schema)
      (struct-put-all m)))
