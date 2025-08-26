(ns xtdb.kafka.connect.util
  (:import (org.apache.kafka.connect.connector ConnectRecord)))

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
