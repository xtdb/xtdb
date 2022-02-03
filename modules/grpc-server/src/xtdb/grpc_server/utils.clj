(ns xtdb.grpc-server.utils
  (:require [clojure.instant :refer [read-instant-date]]))

(defmacro dbg [x] `(let [x# ~x] (println "\n\n\n" '~x "=\n" x# "\n\n\n") x#))

(defn assoc-some
  "Associates a key with a value in a map, if and only if the value is not nil."
  ([m k v]
   (if (nil? v) m (assoc m k {:option {:some v}}))))

(defn nil->default
  "Associates a key with a value in a map, if and only if the value is not nil."
  ([m k v default]
   (if (nil? v) (assoc m k default) (assoc m k v))))

(defn not-nil? [value]
  (not (nil? value)))

(defn ->inst [value]
  (try
    (read-instant-date value)
    (catch Exception _e nil)))