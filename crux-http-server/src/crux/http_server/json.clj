(ns crux.http-server.json
  (:require [clojure.spec.alpha :as s]
            [crux.codec :as c]
            [crux.http-server.entity-ref :as entity-ref]
            [crux.io :as cio]
            [jsonista.core :as j]
            [muuntaja.format.core :as mfc]
            [camel-snake-kebab.core :as csk])
  (:import clojure.lang.IPersistentList
           [com.fasterxml.jackson.core JsonGenerator JsonParser]
           com.fasterxml.jackson.databind.deser.std.StdDeserializer
           com.fasterxml.jackson.databind.DeserializationContext
           com.fasterxml.jackson.databind.module.SimpleModule
           crux.codec.Id
           crux.http_server.entity_ref.EntityRef
           java.io.OutputStream
           jsonista.jackson.PersistentHashMapDeserializer))

(def ^:private crux-jackson-module
  (doto (SimpleModule. "Crux")
    (.addDeserializer java.util.Map
                      (let [jsonista-deser (PersistentHashMapDeserializer.)]
                        (proxy [StdDeserializer] [java.util.Map]
                          (deserialize [^JsonParser p ^DeserializationContext ctxt]
                            (let [res (.deserialize jsonista-deser p ctxt)]
                              (or (when-let [oid (:$oid res)]
                                    (c/new-id (c/hex->id-buffer oid)))
                                  (when-let [b64 (:$base64 res)]
                                    (c/base64-reader b64))
                                  res))))))))

(defn- emit-list [coll ^JsonGenerator gen]
  (if (contains? #{'fn* 'fn} (first coll))
    (.writeString gen (pr-str coll))
    (do
      (.writeStartArray gen)
      (doseq [el coll]
        (.writeObject gen el))
      (.writeEndArray gen))))

(def crux-object-mapper
  (j/object-mapper
   {:encode-key-fn true
    :encoders {Id (fn [crux-id ^JsonGenerator gen]
                    (.writeObject gen {"$oid" (str crux-id)}))
               (Class/forName "[B") (fn [^bytes bytes ^JsonGenerator gen]
                                      (.writeObject gen {"$base64" (c/base64-writer bytes)}))
               EntityRef entity-ref/ref-json-encoder
               IPersistentList emit-list}
    :decode-key-fn true
    :modules [crux-jackson-module]}))

(defn try-decode-json [json]
  (try
    (cond-> json
      (string? json) (j/read-value crux-object-mapper))
    (catch Exception _e
      ::s/invalid)))

(defn camel-case-keys [m]
  (->> m
       (into {} (map (juxt (comp csk/->camelCaseKeyword key) val)))))

(defn ->json-encoder [{:keys [json-encode-fn], :or {json-encode-fn camel-case-keys}}]
  (reify
    mfc/EncodeToBytes
    (encode-to-bytes [_ data _]
      (j/write-value-as-bytes (json-encode-fn data) crux-object-mapper))
    mfc/EncodeToOutputStream
    (encode-to-output-stream [_ {:keys [^Cursor results] :as data} _]
      (fn [^OutputStream output-stream]
        (try
          (if results
            (let [results-seq (iterator-seq results)]
              (j/write-value output-stream (or (some-> results-seq json-encode-fn) '()) crux-object-mapper))
            (j/write-value output-stream data crux-object-mapper))
          (finally
            (cio/try-close results)))))))
