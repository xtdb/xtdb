(ns xtdb.kafka.connect.test.util
  (:require [clojure.test :refer :all]
            [jsonista.core :as json]
            [xtdb.api :as xt])
  (:import (org.apache.avro.generic GenericData$Record GenericRecord)
           (org.apache.kafka.connect.data Struct)
           (org.apache.kafka.connect.sink SinkRecord)))

(defn query-col-types [node table-name]
  (let [col-types-res (xt/q node ["SELECT column_name, data_type
                                   FROM information_schema.columns
                                   WHERE table_name = ?"
                                  table-name])]
    (-> col-types-res
        (->> (map (fn [{:keys [column-name data-type]}]
                    [(keyword column-name) (read-string data-type)]))
             (into {}))
      (dissoc :_valid_from :_valid_to :_system_from :_system_to))))

(defn ->sink-record [{:keys [topic partition
                             key-schema key-value
                             value-schema value-value
                             offset]
                      :or {partition 0 offset 0}}]
  (SinkRecord. topic partition
    key-schema key-value
    value-schema value-value
    offset))

(defn struct-put-all [^Struct s m]
  (reduce
    (fn [^Struct s [k v]]
      (.put s (name k) v))
    s
    m))

(defn ->struct [schema m]
  (-> (Struct. schema)
      (struct-put-all m)))

(defn avro-record-put-all [^GenericRecord r m]
  (reduce
    (fn [^GenericRecord r [k v]]
      (doto r
        (.put (name k) v)))
    r
    m))

(defn ->avro-record [schema m]
  (doto (GenericData$Record. (-> (org.apache.avro.Schema$Parser.)
                                 (.parse (json/write-value-as-string schema))))
    (avro-record-put-all m)))
