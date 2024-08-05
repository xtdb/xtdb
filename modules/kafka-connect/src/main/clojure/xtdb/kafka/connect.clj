(ns xtdb.kafka.connect
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.error :as err]
            [xtdb.api :as xt])
  (:import [org.apache.kafka.connect.data Schema Struct Field]
           org.apache.kafka.connect.sink.SinkRecord
           [java.util Map]
           (xtdb.kafka.connect XtdbSinkConfig)))

(defn- map->edn [m]
  (->> (for [[k v] m]
         [(keyword k)
          (if (instance? Map v)
            (map->edn v)
            v)])
       (into {})))

(defn- get-struct-contents [val]
  (cond
    (instance? Struct val)
    (let [struct-schema (.schema ^Struct val)
          struct-fields (.fields ^Schema struct-schema)]
      (reduce conj
              (map (fn [^Field field] {(keyword (.name field)) (get-struct-contents (.get ^Struct val field))})
                   struct-fields)))
    (instance? java.util.ArrayList val) (into [] (map get-struct-contents val))
    (instance? java.util.HashMap val) (zipmap (map keyword (.keySet ^java.util.HashMap val)) (map get-struct-contents (.values ^java.util.HashMap val)))
    :else val))

(defn- struct->edn [^Struct s]
  (let [output-map (get-struct-contents s)]
    (log/info "map val: " output-map)
    output-map))

(defn- record->edn [^SinkRecord record]
  (let [schema (.valueSchema record)
        value (.value record)]
    (cond
      (and (instance? Struct value) schema)
      (struct->edn value)

      (and (instance? Map value)
           (nil? schema)
           (= #{"payload" "schema"} (set (keys value))))
      (let [payload (.get ^Map value "payload")]
        (cond
          (string? payload)
          (json/parse-string payload true)

          (instance? Map payload)
          (map->edn payload)

          :else
          (throw (err/illegal-arg :unknown-json-payload-type
                                  {::err/message (str "Unknown JSON payload type: " record)}))))

      (instance? Map value)
      (map->edn value)

      (string? value)
      (json/parse-string value true)

      :else
      (throw (err/illegal-arg :unknown-message-type
                              {::err/message (str "Unknown message type: " record)})))))

(defn- find-record-key-eid [^XtdbSinkConfig conf, ^SinkRecord record]
  (let [r-key (.key record)
        id-field (.getIdField conf)]
    (if (nil? r-key)
      (throw (err/illegal-arg :missing-id
                              {::err/message (str "Missing key in record: " record)}))
      (if (instance? Struct r-key)
        (if (= "" id-field)
          (throw (err/illegal-arg :invalid-key-type
                                  {::err/message (str "Invalid key type in record: " record)}))
          (let [r-doc (struct->edn r-key)
                id (get r-doc (keyword id-field))]
            (when-not id
              (throw (err/illegal-arg :missing-id
                                      {::err/message (str "Missing ID in record: " record)})))
            id))
        (if (not= "" id-field)
          (do
            (log/info "id-field:" id-field)
            (throw (err/illegal-arg :invalid-key-type
                                    {::err/message (str "Expected struct key found primitive: " record)})))
          ;; TODO: Check if valid primitive type
          r-key)))))

(defn- find-record-value-eid [^XtdbSinkConfig conf, ^SinkRecord record, doc]
  (let [id-field (.getIdField conf)
        id (get doc (keyword id-field))]
    (when-not id
      (throw (err/illegal-arg :missing-id
                              {::err/message (str "Missing ID in record: " record)})))
    id))

(defn- find-eid [^XtdbSinkConfig conf ^SinkRecord record doc]
  (case (.getIdMode conf)
    "record_key" (find-record-key-eid conf record)
    "record_value" (find-record-value-eid conf record doc)))

(defn- tombstone? [^SinkRecord record]
  (and (nil? (.value record)) (.key record)))

(defn- relation [table valid-from valid-to]
  (cond-> {:into table}
          valid-from (assoc :valid-from valid-from)
          valid-to (assoc :valid-to valid-to)))

(defn table-name [^XtdbSinkConfig conf, ^SinkRecord record]
  (let [topic (.topic record)
        table-name-format (.getTableNameFormat conf)]
    (keyword (str/replace table-name-format "${topic}" topic))))

(defn transform-sink-record [^XtdbSinkConfig conf, ^SinkRecord record]
  (log/info "sink record:" record)
  (let [table (table-name conf record)
        tx-op (if (tombstone? record)
                (if (= "record_key" (.getIdMode conf))
                  (let [id (find-record-key-eid conf record)]
                    [:delete-docs table id])
                  (throw (err/illegal-arg :unsupported-tombstone-mode
                                          {::err/message (str "Unsupported tombstone mode: " record)})))
                (let [doc (record->edn record)
                      id (find-eid conf record doc)
                      valid-from-field (.getValidFromField conf)
                      valid-from (get doc (keyword valid-from-field))
                      valid-to-field (.getValidToField conf)
                      valid-to (get doc (keyword valid-to-field))]
                  [:put-docs (relation table valid-from valid-to) (assoc doc :xt/id id)]))]
    (log/info "tx op:" tx-op)
    tx-op))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn submit-sink-records [api props records]
  (when (seq records)
    (xt/submit-tx api (vec (for [record records]
                              (transform-sink-record props record))))))
