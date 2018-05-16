(ns crux.kafka.nippy
  (:require [taoensso.nippy :as nippy])
  (:import [org.apache.kafka.common.serialization
            Deserializer Serializer]))

(deftype NippySerializer []
  Serializer
  (close [_])
  (configure [_ _ _])
  (serialize [_ _ data]
    (nippy/freeze data)))

(deftype NippyDeserializer []
  Deserializer
  (close [_])
  (configure [_ _ _])
  (deserialize [_ _ data]
    (nippy/thaw data)))
