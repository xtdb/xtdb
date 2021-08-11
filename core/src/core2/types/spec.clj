(ns core2.types.spec
  (:require [clojure.data.json :as json]
            [clojure.spec.alpha :as s])
  (:import [com.fasterxml.jackson.databind ObjectMapper ObjectReader ObjectWriter]
           [org.apache.arrow.vector.types.pojo ArrowType Field Schema]))

;; See https://github.com/apache/arrow/blob/master/format/Schema.fbs

(def ^:private ^ObjectMapper object-mapper (ObjectMapper.))
(def ^:private ^ObjectReader arrow-type-reader (.readerFor object-mapper ArrowType))
(def ^:private ^ObjectReader field-reader (.readerFor object-mapper Field))
(def ^:private ^ObjectWriter writer (.writer object-mapper))

(defmulti arrow-type-spec (comp keyword :name))

(s/def :arrow.type/name (some-fn keyword? string?))
(s/def :arrow/type (s/keys :req-un [:arrow.type/name]))

(defmethod arrow-type-spec :null [_]
  :arrow/type)

(s/def :arrow.type.int/bitWidth #{Byte/SIZE Short/SIZE Integer/SIZE Long/SIZE})
(s/def :arrow.type.int/isSigned boolean?)

(defmethod arrow-type-spec :int [_]
  (s/keys :req-un [:arrow.type/name
                   :arrow.type.int/bitWidth]
          :opt-un [:arrow.type.int/isSigned]))

(s/def :arrow.type.floatingpoint/precision (s/and (s/conformer keyword)
                                                  #{:HALF :SINGLE :DOUBLE}))

(defmethod arrow-type-spec :floatingpoint [_]
  (s/keys :req-un [:arrow.type/name
                   :arrow.type.floatingpoint/precision]))

(defmethod arrow-type-spec :binary [_]
  :arrow/type)

(defmethod arrow-type-spec :utf8 [_]
  :arrow/type)

(defmethod arrow-type-spec :bool [_]
  :arrow/type)

(s/def :arrow.type.decimal/precision nat-int?)
(s/def :arrow.type.decimal/scale nat-int?)
(s/def :arrow.type.decimal/bitWidth #{128 256})

(defmethod arrow-type-spec :decimal [_]
  (s/keys :req-un [:arrow.type/name
                   :arrow.type.decimal/precision
                   :arrow.type.decimal/scale]
          :opt-un [:arrow.type.decimal/bitWidth]))

(s/def :arrow.type.date/unit (s/and (s/conformer keyword)
                                    #{:DAY :MILLISECOND}))

(defmethod arrow-type-spec :date [_]
  (s/keys :req-un [:arrow.type/name]
          :opt-un [:arrow.type.date/unit]))

(s/def :arrow.type.time/unit (s/and (s/conformer keyword)
                                    #{:SECOND :MILLISECOND :MICROSECOND :NANOSECOND}))
(s/def :arrow.type.time/bitWidth #{Integer/SIZE Long/SIZE})

(defmethod arrow-type-spec :time [_]
  (s/keys :req-un [:arrow.type/name]
          :opt-un [:arrow.type.time/unit
                   :arrow.type.time/bitWidth]))

(s/def :arrow.type.timestamp/unit :arrow.type.time/unit)
(s/def :arrow.type.timestamp/timezone string?)

(defmethod arrow-type-spec :timestamp [_]
  (s/keys :req-un [:arrow.type/name
                   :arrow.type.timestamp/unit
                   :arrow.type.timestamp/timezone]))

(s/def :arrow.type.interval/unit (s/and (s/conformer keyword)
                                        #{:YEAR_MONTH :DAY_TIME}))

(defmethod arrow-type-spec :interval [_]
  (s/keys :req-un [:arrow.type/name
                   :arrow.type.interval/unit]))

(defmethod arrow-type-spec :list [_]
  :arrow/type)

(defmethod arrow-type-spec :struct [_]
  :arrow/type)

(s/def :arrow.type.union/mode (s/and (s/conformer keyword)
                                     #{:SPARSE :DENSE}))
(s/def :arrow.type.union/typeIds (s/coll-of nat-int?))

(defmethod arrow-type-spec :union [_]
  (s/keys :req-un [:arrow.type/name
                   :arrow.type.union/mode
                   :arrow.type.union/typeIds]))

(s/def :arrow.type.fixedsizebinary/byteWidth nat-int?)

(defmethod arrow-type-spec :fixedsizebinary [_]
  (s/keys :req-un [:arrow.type/name
                   :arrow.type.fixedsizebinary/byteWidth]))

(s/def :arrow.schema.field.type.fixedsizelist/listSize nat-int?)

(defmethod arrow-type-spec :fixedsizelist [_]
  (s/keys :req-un [:arrow.type/name
                   :arrow.type.fixedsizelist/listSize]))

(s/def :arrow.type.map/keysSorted boolean?)

(defmethod arrow-type-spec :map [_]
  (s/keys :req-un [:arrow.type/name]
          :opt-un [:arrow.type.map/keysSorted]))

(s/def :arrow.type.duration/unit :arrow.type/time-unit)

(defmethod arrow-type-spec :duration [_]
  (s/keys :req-un [:arrow.type/name]
          :opt-un [:arrow.type.duration/unit]))

(defmethod arrow-type-spec :largeutf8 [_]
  :arrow/type)

(defmethod arrow-type-spec :largebinary [_]
  :arrow/type)

(defmethod arrow-type-spec :largelist [_]
  :arrow/type)

(s/def :arrow/key-value (s/map-of string? string?))

(s/def :arrow.field/type (s/multi-spec arrow-type-spec :name))
(s/def :arrow.field/name string?)
(s/def :arrow.field/nullable boolean?)
(s/def :arrow.field/children (s/coll-of :arrow.schema/field))
(s/def :arrow.field/metadata :arrow/key-value)
(s/def :arrow/field (s/keys :req-un [:arrow.field/type]
                            :opt-on [:arrow.field/name :arrow.field/nullable :arrow.field/children :arrow.field/metadata]))

(s/def :arrow.schema/fields (s/coll-of :arrow/field :min-count 1))
(s/def :arrow.schema/metadata :arrow/key-value)
(s/def :arrow/schema (s/keys :req-un [:arrow.schema/fields] :opt-un [:arrow.schema/metadata]))

(defn ->arrow-type ^org.apache.arrow.vector.types.pojo.ArrowType [arrow-type]
  (s/assert :arrow/type arrow-type)
  (.readValue arrow-type-reader (json/write-str arrow-type)))

(defn ->field ^org.apache.arrow.vector.types.pojo.Field [field]
  (s/assert :arrow/field field)
  (.readValue field-reader (json/write-str field)))

(defn ->schema ^org.apache.arrow.vector.types.pojo.Schema [schema]
  (s/assert :arrow/schema schema)
  (Schema/fromJSON (json/write-str schema)))

(defn <-clojure [x]
  (json/read-str (.writeValueAsString writer x) :key-fn keyword))
