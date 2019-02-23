(ns crux.kafka.edn
  (:require [clojure.edn :as edn])
  (:import [org.apache.kafka.common.serialization
            Deserializer Serializer]))

(deftype EDNSerializer []
  Serializer
  (close [_])
  (configure [_ _ _])
  (serialize [_ _ data]
    (some-> data pr-str (.getBytes "UTF-8"))))

(deftype EDNDeserializer []
  Deserializer
  (close [_])
  (configure [_ _ _])
  (deserialize [_ _ data]
    (some-> data (String. "UTF-8") (edn/read-string))))
