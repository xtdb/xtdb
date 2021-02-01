(ns core2.log
  (:require [clojure.java.io :as io])
  (:import [java.io Closeable File]
           java.util.List
           java.util.concurrent.Future))

(definterface LogWriter
  (^java.util.concurrent.Future appendRecord [^bytes record]))

(defprotocol LogReader
  (^java.util.List readRecords [^long from-offset ^int limit]))
