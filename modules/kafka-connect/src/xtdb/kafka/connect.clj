(ns xtdb.kafka.connect
  (:require [juxt.clojars-mirrors.cheshire.v5v10v0.cheshire.core :as json]
            [juxt.clojars-mirrors.cheshire.v5v10v0.cheshire.generate :as json-gen]
            [clojure.tools.logging :as log]
            [xtdb.codec :as c]
            [xtdb.io :as xio]
            [xtdb.error :as err]
            [cognitect.transit :as transit]
            [xtdb.api :as xt])
  (:import [org.apache.kafka.connect.data Schema Struct Field]
           org.apache.kafka.connect.sink.SinkRecord
           org.apache.kafka.connect.source.SourceRecord
           java.io.ByteArrayOutputStream
           [java.util UUID Map]
           [com.fasterxml.jackson.core JsonGenerator JsonParseException]
           xtdb.kafka.connect.XtdbSinkConnector
           xtdb.kafka.connect.XtdbSourceConnector
           xtdb.codec.EDNId))

(json-gen/add-encoder
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

(defn- struct->edn [^Schema schema ^Struct s]
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
      (try
        (doall (json/parse-string value true))
        (catch JsonParseException e
          (log/debug e "Failed to parse as JSON, trying EDN: " value)
          (c/read-edn-string-with-readers value)))

      :else
      (throw (err/illegal-arg :unknown-message-type
                              {::err/message (str "Unknown message type: " record)})))))

(defn- coerce-eid [id]
  (cond
    (and (some? id) (c/valid-id? id))
    (c/id-edn-reader id)

    (string? id)
    (keyword id)))

(defn- find-eid [props ^SinkRecord record doc]
  (let [id (or (get doc :xt/id)
               (some->> (get props XtdbSinkConnector/ID_KEY_CONFIG)
                        (keyword)
                        (get doc))
               (.key record))]
    (or (coerce-eid id)
        (UUID/randomUUID))))

(defn- value->tx-ops [edn-record-value props ^SinkRecord record]
  (cond
    (map? edn-record-value)
    (let [id (find-eid props record edn-record-value)]
      [[::xt/put (assoc edn-record-value :xt/id id)]])

    (vector? edn-record-value) edn-record-value 

    :else (throw (err/illegal-arg :unknown-message-type
                                  {::err/message (str "Unknown edn-record-value: " edn-record-value)}))))

(defn transform-sink-record [props ^SinkRecord record]
  (log/info "sink record:" record)
  (let [tx-ops (if (and (nil? (.value record))
                       (.key record))
                [::xt/delete (coerce-eid (.key record))]
                (let [edn-record-value (record->edn record)]
                  (value->tx-ops edn-record-value props record)))]
    (log/info "tx ops:" tx-ops)
    tx-ops))

(defn submit-sink-records [api props records]
  (when (seq records)
    (doseq [record records]
      (let [tx-ops (transform-sink-record props record)]
        (xt/submit-tx api tx-ops)))))

(defn- write-transit [x]
  (with-open [out (ByteArrayOutputStream.)]
    (let [writer (transit/writer out :json-verbose
                                 {:handlers
                                  {EDNId
                                   (transit/write-handler
                                    "xt/id"
                                    c/edn-id->original-id)}})]
      (transit/write writer x)
      (.toString out))))

(defn- tx-op-with-explicit-valid-time [[op :as tx-op] tx-time]
  (or (case op
        ::xt/put
        (when (= 2 (count tx-op))
          (conj tx-op tx-time))
        ::xt/delete
        (when (= 2 (count tx-op))
          (conj tx-op tx-time))
        ::xt/match
        (when (= 2 (count tx-op))
          (conj tx-op tx-time))
        ::xt/cas
        (when (= 3 (count tx-op))
          (conj tx-op tx-time))
        nil)
      tx-op))

(defn- tx-log-entry->tx-source-records [source-partition topic formatter
                                        {::xt/keys [tx-ops tx-id tx-time] :as tx}]
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
    ::xt/put
    (when (= 2 (count tx-op))
      (let [[_ new-doc] tx-op]
        [(:xt/id new-doc)
         new-doc]))
    (::xt/delete ::xt/evict)
    (when (= 2 (count tx-op))
      (let [[_ deleted-id] tx-op]
        [deleted-id]))
    ::xt/cas
    (when (= 3 (count tx-op))
      (let [[_ old-doc new-doc] tx-op]
        [(:xt/id new-doc)
         new-doc]))))

(defn- tx-log-entry->doc-source-records [source-partition topic formatter
                                         {::xt/keys [tx-ops tx-id tx-time], :as tx}]
  (log/info "tx-ops:" tx-ops)
  (for [[op :as tx-op] tx-ops
        :when (not (contains? #{::xt/fn ::xt/match} op))
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

(defn poll-source-records [api source-offset props]
  (let [url (get props XtdbSourceConnector/URL_CONFIG)
        topic (get props XtdbSourceConnector/TOPIC_CONFIG)
        format (get props XtdbSourceConnector/FORMAT_CONFIG)
        mode (get props XtdbSourceConnector/MODE_CONFIG)
        batch-size (get props XtdbSourceConnector/TASK_BATCH_SIZE_CONFIG)
        source-partition {"url" url}
        formatter (case format
                    "edn" xio/pr-edn-str
                    "json" json/generate-string
                    "transit" write-transit)
        tx-log-entry->source-records (case mode
                                       "tx" tx-log-entry->tx-source-records
                                       "doc" tx-log-entry->doc-source-records)
        after-tx-id (some-> (get source-offset "offset") long)]
    (with-open [tx-log-iterator (xt/open-tx-log api after-tx-id true)]
      (log/info "source offset:" source-offset "tx-id:" after-tx-id "format:" format "mode:" mode)
      (let [records (->> (iterator-seq tx-log-iterator)
                         (take (Long/parseLong batch-size))
                         (map #(tx-log-entry->source-records source-partition topic formatter %))
                         (reduce into []))]
        (when (seq records)
          (log/info "source records:" records))
        records))))
