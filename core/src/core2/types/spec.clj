(ns core2.types.spec
  (:require [clojure.data.json :as json]
            [clojure.spec.alpha :as s])
  (:import [com.fasterxml.jackson.databind ObjectMapper ObjectReader ObjectWriter]
           [org.apache.arrow.vector.types.pojo ArrowType Field Schema]))

(def ^:private ^ObjectMapper object-mapper (ObjectMapper.))
(def ^:private ^ObjectReader arrow-type-reader (.readerFor object-mapper ArrowType))
(def ^:private ^ObjectReader field-reader (.readerFor object-mapper Field))
(def ^:private ^ObjectWriter writer (.writer object-mapper))

(defmulti arrow-type-spec (comp keyword :name))

(s/def :arrow.schema.field.type.int/bitWidth #{Byte/SIZE Short/SIZE Integer/SIZE Long/SIZE})
(s/def :arrow.schema.field.type.int/isSigned boolean?)

(defmethod arrow-type-spec :null [_]
  (s/keys :req-un [:arrow.schema.field.type/name]))

(defmethod arrow-type-spec :int [_]
  (s/keys :req-un [:arrow.schema.field.type/name
                   :arrow.schema.field.type.int/bitWidth]
          :opt-un [:arrow.schema.field.type.int/isSigned]))

(s/def :arrow.schema.field.type.floatingpoint/precision (s/and (s/conformer keyword)
                                                               #{:HALF :SINGLE :DOUBLE}))

(defmethod arrow-type-spec :floatingpoint [_]
  (s/keys :req-un [:arrow.schema.field.type/name
                   :arrow.schema.field.type.floatingpoint/precision]))

(defmethod arrow-type-spec :binary [_]
  (s/keys :req-un [:arrow.schema.field.type/name]))

(defmethod arrow-type-spec :utf8 [_]
  (s/keys :req-un [:arrow.schema.field.type/name]))

(defmethod arrow-type-spec :bool [_]
  (s/keys :req-un [:arrow.schema.field.type/name]))

(s/def :arrow.schema.field.type.decimal/precision nat-int?)
(s/def :arrow.schema.field.type.decimal/scale nat-int?)
(s/def :arrow.schema.field.type.decimal/bitWidth #{128 256})

(defmethod arrow-type-spec :decimal [_]
  (s/keys :req-un [:arrow.schema.field.type/name
                   :arrow.schema.field.type.decimal/precision
                   :arrow.schema.field.type.decimal/scale]
          :opt-un [:arrow.schema.field.type.decimal/bitWidth]))

(s/def :arrow.schema.field.type.date/unit (s/and (s/conformer keyword)
                                                 #{:DAY :MILLISECOND}))

(defmethod arrow-type-spec :date [_]
  (s/keys :req-un [:arrow.schema.field.type/name
                   :arrow.schema.field.type.date/unit]))

(s/def :arrow.schema.field.type/time-unit (s/and (s/conformer keyword)
                                                 #{:SECOND :MILLISECOND :MICROSECOND :NANOSECOND}))

(s/def :arrow.schema.field.type.time/unit :arrow.schema.field.type/time-unit)
(s/def :arrow.schema.field.type.time/bitWidth #{Integer/SIZE Long/SIZE})

(defmethod arrow-type-spec :time [_]
  (s/keys :req-un [:arrow.schema.field.type/name
                   :arrow.schema.field.type.time/unit
                   :arrow.schema.field.type.time/bitWidth]))

(s/def :arrow.schema.field.type.timestamp/unit :arrow.schema.field.type/time-unit)
(s/def :arrow.schema.field.type.timestamp/timezone string?)

(defmethod arrow-type-spec :timestamp [_]
  (s/keys :req-un [:arrow.schema.field.type/name
                   :arrow.schema.field.type.timestamp/unit
                   :arrow.schema.field.type.timestamp/timezone]))

(s/def :arrow.schema.field.type.interval/unit (s/and (s/conformer keyword)
                                                     #{:YEAR_MONTH :DAY_TIME}))

(defmethod arrow-type-spec :interval [_]
  (s/keys :req-un [:arrow.schema.field.type/name
                   :arrow.schema.field.type.interval/unit]))

(s/def :arrow.schema.field.type.duration/unit :arrow.schema.field.type/time-unit)

(defmethod arrow-type-spec :duration [_]
  (s/keys :req-un [:arrow.schema.field.type/name]
          :opt-un [:arrow.schema.field.type.duration/unit]))


(s/def :arrow.schema.field.type.fixedsizebinary/byteWidth nat-int?)

(defmethod arrow-type-spec :fixedsizebinary [_]
  (s/keys :req-un [:arrow.schema.field.type/name
                   :arrow.schema.field.type.fixedsizebinary/byteWidth]))

(s/def :arrow.schema.field.type.fixedsizelist/listSize nat-int?)

(defmethod arrow-type-spec :fixedsizelist [_]
  (s/keys :req-un [:arrow.schema.field.type/name
                   :arrow.schema.field.type.fixedsizelist/listSize]))

(s/def :arrow.schema.field.type.map/keysSorted boolean?)

(defmethod arrow-type-spec :map [_]
  (s/keys :req-un [:arrow.schema.field.type/name]
          :opt-un [:arrow.schema.field.type.map/keysSorted]))

(defmethod arrow-type-spec :largeutf8 [_]
  (s/keys :req-un [:arrow.schema.field.type/name]))

(defmethod arrow-type-spec :largebinary [_]
  (s/keys :req-un [:arrow.schema.field.type/name]))

(defmethod arrow-type-spec :largelist [_]
  (s/keys :req-un [:arrow.schema.field.type/name]))

(s/def :arrow.schema.field.type/name (some-fn keyword? string?))
(s/def :arrow.schema.field/type (s/multi-spec arrow-type-spec :name))

(s/def :arrow.schema.field/name string?)
(s/def :arrow.schema.field/nullable boolean?)
(s/def :arrow.schema.field/children (s/coll-of :arrow.schema/field))
(s/def :arrow.schema/field (s/keys :req-un [:arrow.schema.field/name :arrow.schema.field/type]
                                   :opt-on [:arrow.schema.field/nullable :arrow.schema.field/children]))

(s/def :arrow.schema/fields (s/coll-of :arrow.schema/field :min-count 1))
(s/def :arrow.schama/metadata (s/map-of string? string?))
(s/def :arrow/schema (s/keys :req-un [:arrow.schema/fields] :opt-un [:arrow.schema/metadata]))

(defn ->arrow-type ^org.apache.arrow.vector.types.pojo.ArrowType [arrow-type]
  (s/assert :arrow.schema.field/type arrow-type)
  (.readValue arrow-type-reader (json/write-str arrow-type)))

(defn ->field ^org.apache.arrow.vector.types.pojo.Field [field]
  (s/assert :arrow.schema/field field)
  (.readValue field-reader (json/write-str field)))

(defn ->schema ^org.apache.arrow.vector.types.pojo.Schema [schema]
  (s/assert :arrow/schema schema)
  (Schema/fromJSON (json/write-str schema)))

(defn <-clojure [x]
  (json/read-str (.writeValueAsString writer x) :key-fn keyword))
