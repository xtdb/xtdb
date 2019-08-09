(ns crux.kafka.connect
  (:require [cheshire.core :as json]
            [clojure.string :as str])
  (:import org.apache.kafka.connect.sink.SinkRecord
           java.util.UUID))

(defn transform-sink-record [props ^SinkRecord record]
  (let [doc (json/parse-string (.value record) true)
        id (get doc :crux.db/id (UUID/randomUUID))]
    [:crux.tx/put (assoc doc :crux.db/id id)]))
