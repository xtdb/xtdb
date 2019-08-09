(ns crux.kafka.connect
  (:require [cheshire.core :as json]
            [clojure.string :as str])
  (:import org.apache.kafka.connect.sink.SinkRecord))

(defn transform-sink-record [^SinkRecord record]
  (str/upper-case (.value record)))
