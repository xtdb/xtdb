(ns core2.object-store
  (:require [clojure.java.io :as io])
  (:import [java.io Closeable File]
           java.util.concurrent.Future))

(set! *unchecked-math* :warn-on-boxed)

(definterface ObjectStore
  (^java.util.concurrent.Future getObject [^String k])
  (^java.util.concurrent.Future putObject [^String k v])
  (^java.util.concurrent.Future listObjects [])
  (^java.util.concurrent.Future deleteObject [^String k]))

(deftype LocalDirectoryObjectStore [^File dir]
  ObjectStore
  (getObject [this k]
    (future
      (let [f (io/file dir k)]
        (when (.isFile f)
          (io/as-url f)))))

  (putObject [this k v]
    (future
      (io/as-url (doto (io/file dir k)
                   (io/make-parents)
                   (->> (io/copy v))))))

  (listObjects [this]
    (future
      (let [dir-path (.toPath dir)]
        (for [^File f (file-seq dir)
              :when (.isFile f)]
          (str (.relativize dir-path (.toPath f)))))))

  (deleteObject [this k]
    (future
      (let [f (io/file dir k)]
        (if (.isFile f)
          (.delete f)
          (or (let [parent (.getParentFile f)]
                (when (empty? (.listFiles parent))
                  (.delete parent)))
              false)))))

  Closeable
  (close [this]))

(defn ->local-directory-object-store ^core2.object_store.LocalDirectoryObjectStore [dir]
  (->LocalDirectoryObjectStore (io/file dir)))
