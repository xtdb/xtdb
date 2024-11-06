(ns xtdb.object-store
  (:import [java.nio.file Path]
           xtdb.api.storage.ObjectStore$StoredObject))

(set! *unchecked-math* :warn-on-boxed)

(defn obj-missing-exception [k]
  (IllegalStateException. (format "Object '%s' doesn't exist." k)))

(defrecord StoredObject [^Path k, ^long size]
  ObjectStore$StoredObject
  (getKey [_] k)
  (getSize [_] size))
