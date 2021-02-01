(ns core2.log
  (:require [clojure.java.io :as io])
  (:import [java.io Closeable File]
           java.util.List
           java.util.concurrent.Future))

(definterface LogWriter
  (^java.util.concurrent.Future appendRecord [^bytes record]))

(defprotocol LogReader
  (^java.util.List readRecords [^long from-offset ^int limit]))

#_(deftype LocalDirectoryLog [^File dir]
    LogWriter
    (appendRecord [this record]
      (future))

    LogReader
    (readRecords [this from-offset limit]
      ))

#_(defn ->local-directory-log ^core2.object_store.LocalDirectoryLog [dir]
    (->LocalDirectoryLog (io/file dir)))
