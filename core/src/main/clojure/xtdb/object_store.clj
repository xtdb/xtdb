(ns xtdb.object-store
  (:require [xtdb.util :as util])
  (:import [java.io Writer]
           xtdb.api.storage.ObjectStore$StoredObject))

(set! *unchecked-math* :warn-on-boxed)

(defn obj-missing-exception [k]
  (IllegalStateException. (format "Object '%s' doesn't exist." k)))

(defn ->StoredObject [k size]
  (ObjectStore$StoredObject. (util/->path k) size))

(defn <-StoredObject [^ObjectStore$StoredObject obj]
  {:key (.getKey obj), :size (.getSize obj)})

(defmethod print-method ObjectStore$StoredObject [obj, ^Writer w]
  (.write w "#xt/stored-obj ")
  (print-method (<-StoredObject obj) w))

(defn map->StoredObject [{:keys [k size]}]
  (->StoredObject (util/->path k) size))
