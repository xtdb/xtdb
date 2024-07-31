(ns xtdb.kafka.connect
  (:require [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [xtdb.error :as err]
            [xtdb.api :as xt]
            ; For XtdbSinkTask
            [xtdb.client])
  (:import [org.apache.kafka.connect.data Schema Struct Field]
           org.apache.kafka.connect.sink.SinkRecord
           [java.util Map]
           xtdb.kafka.connect.XtdbSinkConnector))

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

(defn- struct->edn [^Schema _schema ^Struct s]
  (let [ output-map (get-struct-contents s)]
    (log/info "map val: " output-map)
    output-map))

(defn- record->edn [^SinkRecord record]
  (let [schema (.valueSchema record)
        value (.value record)]
    (cond
      (and (instance? Struct value) schema)
      (struct->edn schema value)

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

(defn- find-eid [props ^SinkRecord record doc]
  (let [id (or (get doc :xt/id)
               (some->> (get props XtdbSinkConnector/ID_KEY_CONFIG)
                        (keyword)
                        (get doc))
               (.key record))]
    (when-not id
      (throw (err/illegal-arg :missing-id
                              {::err/message (str "Missing ID in record: " record)})))
    id))


(defn- tombstone? [^SinkRecord record]
  (and (nil? (.value record)) (.key record)))

(defn transform-sink-record [props ^SinkRecord record]
  (log/info "sink record:" record)
  (let [topic (keyword (.topic record))
        tx-op (if (tombstone? record)
                [:delete-docs topic (.key record)]
                (let [doc (record->edn record)
                      id (find-eid props record doc)]
                  [:put-docs topic (assoc doc :xt/id id)]))]
    (log/info "tx op:" tx-op)
    tx-op))

(defn submit-sink-records [api props records]
  (when (seq records)
    (xt/submit-tx api (vec (for [record records]
                              (transform-sink-record props record))))))
