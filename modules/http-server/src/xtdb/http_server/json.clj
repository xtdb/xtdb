(ns xtdb.http-server.json
  (:require [clojure.spec.alpha :as s]
            [xtdb.codec :as c]
            [xtdb.http-server.entity-ref :as entity-ref]
            [xtdb.io :as xio]
            [juxt.clojars-mirrors.jsonista.v0v3v1.jsonista.core :as j]
            [juxt.clojars-mirrors.camel-snake-kebab.v0v4v2.camel-snake-kebab.core :as csk]
            [juxt.clojars-mirrors.muuntaja.v0v6v8.muuntaja.format.core :as mfc])
  (:import clojure.lang.IPersistentList
           com.fasterxml.jackson.core.JsonGenerator
           [xtdb.codec EDNId Id]
           xtdb.http_server.entity_ref.EntityRef
           java.io.OutputStream))

(defn- emit-list [coll ^JsonGenerator gen]
  (if (contains? #{'fn* 'fn} (first coll))
    (.writeString gen (pr-str coll))
    (do
      (.writeStartArray gen)
      (doseq [el coll]
        (.writeObject gen el))
      (.writeEndArray gen))))

(def xtdb-object-mapper
  (j/object-mapper
   {:encode-key-fn true
    :encoders {Id (fn [xtdb-id ^JsonGenerator gen]
                    (.writeString gen (str xtdb-id)))
               EDNId (fn [xtdb-id ^JsonGenerator gen]
                       (.writeString gen (str xtdb-id)))
               (Class/forName "[B") (fn [^bytes bytes ^JsonGenerator gen]
                                      (.writeString gen (c/base64-writer bytes)))
               EntityRef entity-ref/ref-json-encoder
               IPersistentList emit-list}
    :decode-key-fn true}))

(defn try-decode-json [json]
  (try
    (cond-> json
      (string? json) (j/read-value xtdb-object-mapper))
    (catch Exception _e
      ::s/invalid)))

(defn camel-case-keys [m]
  (cond->> m
    (map? m) (into {} (map (juxt (comp csk/->camelCaseKeyword key) val)))))

(defn ->json-encoder [{:keys [json-encode-fn], :or {json-encode-fn identity}}]
  (reify
    mfc/EncodeToBytes
    (encode-to-bytes [_ data _]
      (j/write-value-as-bytes (json-encode-fn data) xtdb-object-mapper))
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [^Cursor results] :as data} _]
      (fn [^OutputStream output-stream]
        (try
          (j/write-value output-stream
                         (if results
                           (map json-encode-fn (iterator-seq results))
                           (json-encode-fn data))
                         xtdb-object-mapper)
          (finally
            (xio/try-close results)))))))

(defn write-str [v]
  (j/write-value-as-string v))
