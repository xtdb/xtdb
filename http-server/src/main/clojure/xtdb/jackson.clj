(ns xtdb.jackson
  (:require [jsonista.core :as json]
            [jsonista.tagged :as jt])
  (:import (clojure.lang Keyword PersistentHashSet)
           (com.fasterxml.jackson.core JsonGenerator)
           (com.fasterxml.jackson.databind ObjectMapper)
           (com.fasterxml.jackson.databind.module SimpleModule)
           (java.time Instant Duration LocalDate LocalDateTime ZonedDateTime)
           (java.util Date)
           (jsonista.jackson FunctionalSerializer)))

(defn serializer ^FunctionalSerializer [^String tag encoder]
  (FunctionalSerializer.
   (fn [value ^JsonGenerator gen]
     (.writeStartObject gen)
     (.writeStringField gen "@type" tag)
     (.writeFieldName gen "@value")
     (encoder value gen)
     (.writeEndObject gen))))

(defn encode-throwable [^Throwable t ^JsonGenerator gen]
  (let [mapper ^ObjectMapper (.getCodec gen)]
    (.writeStartObject gen)
    (.writeStringField gen "xtdb.error/message" (.getMessage t))
    (.writeStringField gen "xtdb.error/class" (.getName (.getClass t)))
    (.writeFieldName gen "xtdb.error/data")
    (.writeRawValue gen (.writeValueAsString mapper (ex-data t)))
    (.writeEndObject gen)))

(defn json-ld-module
  "See jsonista.tagged/module but for Json-Ld reading/writing."
  ^SimpleModule
  [{:keys [handlers]}]
  (let [_decoders (->> (for [[_ {:keys [tag decode]}] handlers] [tag decode]) (into {}))]
    (reduce-kv
     (fn [^SimpleModule module t {:keys [tag encode] :or {encode jt/encode-str}}]
       (.addSerializer module t (serializer tag encode)))
     (doto (SimpleModule. "JSON-LD")
       ;; TODO add a proper deserializer
       #_(.addDeserializer Map (JsonLdOrPersistentHashMapDeserializer. decoders)))
     handlers)))

(def handlers {Keyword {:tag "xt:keyword"
                        :encode jt/encode-keyword}
               PersistentHashSet {:tag "xt:set"
                                  :encode jt/encode-collection}
               Date {:tag "xt:timestamp"
                     :encode #(str (.toInstant ^Date %))}
               LocalDate {:tag "xt:date"}
               Duration {:tag "xt:duration"}
               LocalDateTime {:tag "xt:timestamp"}
               ZonedDateTime {:tag "xt:timestamptz"}
               Instant {:tag "xt:timestamp"}
               Throwable {:tag "xt:error"
                          :encode encode-throwable}})

(comment
  (def mapper
    (json/object-mapper
     {:encode-key-fn true
      :decode-key-fn true
      :modules [(json-ld-module {:handlers handlers})]}))

  (json/write-value-as-string {:foo :bar :toto #{:foo :toto}} mapper))
