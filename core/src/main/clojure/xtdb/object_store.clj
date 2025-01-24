(ns xtdb.object-store
  (:require [xtdb.util :as util])
  (:import xtdb.api.storage.ObjectStore$StoredObject))

(set! *unchecked-math* :warn-on-boxed)

(defn obj-missing-exception [k]
  (IllegalStateException. (format "Object '%s' doesn't exist." k)))

(defn ->StoredObject [k size]
  (ObjectStore$StoredObject. (util/->path k) size))

(defn map->StoredObject [{:keys [k size]}]
  (->StoredObject (util/->path k) size))
