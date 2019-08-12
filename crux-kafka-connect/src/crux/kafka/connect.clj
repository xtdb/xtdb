(ns crux.kafka.connect
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [crux.codec :as c])
  (:import org.apache.kafka.connect.data.Struct
           org.apache.kafka.connect.sink.SinkRecord
           [java.util UUID Map]
           crux.api.ICruxAPI))

(defn- map->edn [m]
  (->> (for [[k v] m]
         [(keyword k)
          (if (instance? Map v)
            (map->edn v)
            v)])
       (into {})))

(defn- record->edn [^SinkRecord record]
  (let [value (.value record)]
    (log/info "Raw Value:" value)
    (cond
      (and (instance? Struct value)
           (.valueSchema record))
      (throw (UnsupportedOperationException. "Avro conversion not yet supported."))
      (instance? Map value)
      (map->edn value)
      (string? value)
      (json/parse-string (.value record) true)
      :else
      (throw (IllegalArgumentException. (str "Unknown message type: " record))))))

(defn transform-sink-record [props ^SinkRecord record]
  (let [doc (record->edn record)
        id (or (.key record)
               (get doc (or (some-> (get props "id.key") (keyword))
                            :crux.db/id)))
        id (cond
             (c/valid-id? id)
             (c/id-edn-reader id)
             (string? id)
             (keyword id)
             :else
             (UUID/randomUUID))]
    [:crux.tx/put (assoc doc :crux.db/id id)]))

(defn submit-sink-records [^ICruxAPI api props records]
  (when (seq records)
    (.submitTx api (vec (for [record records]
                          (transform-sink-record props record))))))
