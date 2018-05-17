(ns crux.kafka.nippy
  (:import [org.apache.kafka.common.serialization
            Deserializer Serializer]))

(def freeze)
(def thaw)

(when-not *compile-files*
  (require 'taoensso.nippy)
  (alter-var-root #'freeze (constantly (resolve 'taoensso.nippy/freeze)))
  (alter-var-root #'thaw (constantly (resolve 'taoensso.nippy/thaw))))

(deftype NippySerializer []
  Serializer
  (close [_])
  (configure [_ _ _])
  (serialize [_ _ data]
    (freeze data)))

(deftype NippyDeserializer []
  Deserializer
  (close [_])
  (configure [_ _ _])
  (deserialize [_ _ data]
    (thaw data)))
