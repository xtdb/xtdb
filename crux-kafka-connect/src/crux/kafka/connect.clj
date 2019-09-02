(ns crux.kafka.connect
  (:require [cheshire.core :as json]
            [cheshire.generate]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [cognitect.transit :as transit])
  (:import [org.apache.kafka.connect.data Schema Struct]
           org.apache.kafka.connect.sink.SinkRecord
           org.apache.kafka.connect.source.SourceRecord
           java.io.ByteArrayOutputStream
           [java.util UUID Map]
           [com.fasterxml.jackson.core JsonGenerator JsonParseException]
           crux.kafka.connect.CruxSinkConnector
           crux.kafka.connect.CruxSourceConnector
           crux.codec.EDNId
           crux.api.ICruxAPI))

(cheshire.generate/add-encoder
 EDNId
 (fn [c ^JsonGenerator json-generator]
   (.writeString json-generator (str (c/edn-id->original-id c)))))

(defn- map->edn [m]
  (->> (for [[k v] m]
         [(keyword k)
          (if (instance? Map v)
            (map->edn v)
            v)])
       (into {})))

(defn- struct->edn [^Schema schema ^Struct s]
  (throw (UnsupportedOperationException. "Struct conversion not yet supported.")))

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
          (throw (IllegalArgumentException. (str "Unknown JSON payload type: " record)))))

      (instance? Map value)
      (map->edn value)

      (string? value)
      (try
        (json/parse-string value true)
        (catch JsonParseException e
          (log/debug e "Failed to parse as JSON, trying EDN: " value)
          (c/read-edn-string-with-readers value)))

      :else
      (throw (IllegalArgumentException. (str "Unknown message type: " record))))))

(defn- coerce-eid [id]
  (cond
    (and id (c/valid-id? id))
    (c/id-edn-reader id)

    (string? id)
    (keyword id)))

(defn- find-eid [props ^SinkRecord record doc]
  (let [id (or (get doc :crux.db/id)
               (some->> (get props CruxSinkConnector/ID_KEY_CONFIG)
                        (keyword)
                        (get doc))
               (.key record))]
    (or (coerce-eid id)
        (UUID/randomUUID))))

(defn transform-sink-record [props ^SinkRecord record]
  (log/info "sink record:" record)
  (let [tx-op (if (and (nil? (.value record))
                       (.key record))
                [:crux.tx/delete (coerce-eid (.key record))]
                (let [doc (record->edn record)
                      id (find-eid props record doc)]
                  [:crux.tx/put (assoc doc :crux.db/id id)]))]
    (log/info "tx op:" tx-op)
    tx-op))

(defn submit-sink-records [^ICruxAPI api props records]
  (when (seq records)
    (.submitTx api (vec (for [record records]
                          (transform-sink-record props record))))))

(defn- write-transit [x]
  (with-open [out (ByteArrayOutputStream.)]
    (let [writer (transit/writer out :json-verbose
                                 {:handlers
                                  {EDNId
                                   (transit/write-handler
                                    "crux/id"
                                    c/edn-id->original-id)}})]
      (transit/write writer x)
      (.toString out))))

(defn- tx-op-with-explicit-valid-time [[op :as tx-op] tx-time]
  (or (case op
        :crux.tx/put
        (when (= 2 (count tx-op))
          (conj tx-op tx-time))
        :crux.tx/delete
        (when (= 2 (count tx-op))
          (conj tx-op tx-time))
        :crux.tx/cas
        (when (= 3 (count tx-op))
          (conj tx-op tx-time)))
      tx-op))

(defn- tx-log-entry->tx-source-records [source-partition topic formatter {:keys [crux.api/tx-ops
                                                                                 crux.tx/tx-id
                                                                                 crux.tx/tx-time]
                                                                          :as tx}]
  [(SourceRecord. source-partition
                  {"offset" tx-id}
                  topic
                  nil
                  nil
                  nil
                  Schema/STRING_SCHEMA
                  (->> (for [tx-op tx-ops]
                         (tx-op-with-explicit-valid-time tx-op tx-time))
                       (vec)
                       (formatter))
                  (inst-ms tx-time))])

(defn- tx-op->id+doc [[op :as tx-op]]
  (case op
    :crux.tx/put
    (when (= 2 (count tx-op))
      (let [[_ new-doc] tx-op]
        [(:crux.db/id new-doc)
         new-doc]))
    (:crux.tx/delete :crux.tx/evict)
    (when (= 2 (count tx-op))
      (let [[_ deleted-id] tx-op]
        [deleted-id]))
    :crux.tx/cas
    (when (= 3 (count tx-op))
      (let [[_ old-doc new-doc] tx-op]
        [(:crux.db/id new-doc)
         new-doc]))))

(defn- tx-log-entry->doc-source-records [source-partition topic formatter {:keys [crux.api/tx-ops
                                                                                  crux.tx/tx-id
                                                                                  crux.tx/tx-time]
                                                                           :as tx}]
  (log/info "tx-ops:" tx-ops)
  (for [[op :as tx-op] tx-ops
        :when (not= :crux.tx/fn op)
        :let [[id doc] (tx-op->id+doc tx-op)
              hashed-id (str (c/new-id id))
              _ (log/info "tx-op:" tx-op "id:" id "hashed id:" hashed-id "doc:" doc)]
        :when id]
    (SourceRecord. source-partition
                   {"offset" tx-id}
                   topic
                   nil
                   Schema/STRING_SCHEMA
                   hashed-id
                   Schema/OPTIONAL_STRING_SCHEMA
                   (some-> doc (formatter))
                   (inst-ms tx-time))))

(defn poll-source-records [^ICruxAPI api source-offset props]
  (with-open [tx-log-context (.newTxLogContext api)]
    (let [url (get props CruxSourceConnector/URL_CONFIG)
          topic (get props CruxSourceConnector/TOPIC_CONFIG)
          format (get props CruxSourceConnector/FORMAT_CONFIG)
          mode (get props CruxSourceConnector/MODE_CONFIG)
          batch-size (get props CruxSourceConnector/TASK_BATCH_SIZE_CONFIG)
          source-partition {"url" url}
          formatter (case format
                      "edn" pr-str
                      "json" json/generate-string
                      "transit" write-transit)
          tx-log-entry->source-records (case mode
                                         "tx" tx-log-entry->tx-source-records
                                         "doc" tx-log-entry->doc-source-records)
          from-tx-id (inc (long (or (get source-offset "offset") -1)))]
      (log/info "source offset:" source-offset "tx-id:" from-tx-id "format:" format "mode:" mode)
      (let [records (->> (.txLog api tx-log-context from-tx-id true)
                         (take (Long/parseLong batch-size))
                         (map #(tx-log-entry->source-records source-partition topic formatter %))
                         (reduce into []))]
        (when (seq records)
          (log/info "source records:" records))
        records))))
