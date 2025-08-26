(ns xtdb.kafka.connect.encode
  (:require [clojure.string :as str]
            [cognitect.transit :as transit]
            [xtdb.kafka.connect.util :refer [clone-connect-record]])
  (:import (java.util List)
           (org.apache.kafka.connect.connector ConnectRecord)
           (org.apache.kafka.connect.data Field Schema Schema$Type Struct)))

(defn ?encode-by-xtdb-type [^Schema schema data]
  (assert (some? data))
  (when-some [xtdb-type (some-> schema .parameters (get "xtdb.type"))]
    (case xtdb-type
      "interval" (transit/tagged-value "xtdb/interval" data)
      "timestamptz" (transit/tagged-value "time/zoned-date-time" data)
      (transit/tagged-value xtdb-type data))))

(defn ?encode-by-simple-type [^Schema schema data]
  (assert (some? data))
  (condp = (.type schema)
    Schema$Type/INT32 (transit/tagged-value "i32" data)
    Schema$Type/INT16 (transit/tagged-value "i16" data)
    Schema$Type/INT8 (transit/tagged-value "i8" data)

    Schema$Type/FLOAT64 (transit/tagged-value "f64" data)
    Schema$Type/FLOAT32 (transit/tagged-value "f32" data)

    nil))

(defn encode-by-schema* [^Schema schema, data, path]
  (try
    (cond
      (nil? schema)
      data

      (-> schema .type (= Schema$Type/STRUCT))
      (if-not (instance? Struct data)
        (throw (IllegalArgumentException. "expected Struct"))
        (reduce
          (fn [m ^Field field]
            (assoc m (.name field) (encode-by-schema* (.schema field)
                                                      (.get data field)
                                                      (conj path (.name field)))))
          {}
          (.fields schema)))

      (-> schema .type (= Schema$Type/MAP))
      (if-not (map? data)
        (throw (IllegalArgumentException. "expected Map"))
        (reduce-kv
          (fn [m k v]
            (let [subpath (conj path (name k))]
              (assoc m
                (encode-by-schema* (.keySchema schema) k subpath)
                (encode-by-schema* (.valueSchema schema) v subpath))))
          {}
          data))

      (-> schema .type (= Schema$Type/ARRAY))
      (if-not (or (sequential? data)
                  (instance? List data))
        (throw (IllegalArgumentException. "expected array"))
        (map-indexed
          (fn [i x]
            (encode-by-schema* (.valueSchema schema) x (conj path i)))
          data))

      (nil? data)
      nil

      :else
      (or (?encode-by-xtdb-type schema data)
          (?encode-by-simple-type schema data)
          data))

    (catch Exception e
      (if (-> e ex-data ::path)
        (throw e)
        (throw (ex-info (str "path [/" (str/join '/ path) "]: " (ex-message e))
                 {::path path}
                 e))))))

(defn encode-by-schema [^Schema schema, data]
  (encode-by-schema* schema data []))

(defn ^ConnectRecord encode-record-value-by-schema [^ConnectRecord record]
  (clone-connect-record record {:value-schema nil
                                :value (encode-by-schema (.valueSchema record) (.value record))}))
