(ns crux.kafka.connect
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [crux.codec :as c])
  (:import org.apache.kafka.connect.sink.SinkRecord
           java.util.UUID
           crux.api.ICruxAPI))

(defn transform-sink-record [props ^SinkRecord record]
  (let [doc (json/parse-string (.value record) true)
        id (get doc (or (some-> (get props "id.key") (keyword))
                        :crux.db/id))
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
