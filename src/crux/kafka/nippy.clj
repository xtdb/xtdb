(ns crux.kafka.nippy
  (:import [org.apache.kafka.common.serialization
            Deserializer Serializer]))

(def freeze)
(def thaw)

;; TODO: This won't likely work under Graal, so maybe just move this
;; to a set of Java classes? The reason this is resolved lazily is due
;; wanting to avoid AOT of Nippy, but this will happen anyway when
;; using Graal.

(when-not *compile-files*
  (require 'taoensso.nippy)
  (alter-var-root #'freeze (constantly (resolve 'taoensso.nippy/fast-freeze)))
  (alter-var-root #'thaw (constantly (resolve 'taoensso.nippy/fast-thaw))))

(deftype NippySerializer []
  Serializer
  (close [_])
  (configure [_ _ _])
  (serialize [_ _ data]
    (some-> data freeze)))

(deftype NippyDeserializer []
  Deserializer
  (close [_])
  (configure [_ _ _])
  (deserialize [_ _ data]
    (some-> data thaw)))
