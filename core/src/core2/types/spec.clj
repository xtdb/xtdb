(ns core2.types.spec
  (:require [clojure.data.json :as json]
            [clojure.spec.alpha :as s]
            [clojure.core.protocols :as p])
  (:import [com.fasterxml.jackson.databind ObjectMapper ObjectReader ObjectWriter]
           [org.apache.arrow.vector.types.pojo ArrowType Field FieldType Schema]
           java.time.ZoneId))

;; See https://github.com/apache/arrow/blob/master/format/Schema.fbs

(def ^:private ^ObjectMapper object-mapper (ObjectMapper.))
(def ^:private ^ObjectReader arrow-type-reader (.readerFor object-mapper ArrowType))
(def ^:private ^ObjectWriter object-writer (.writer object-mapper))

(defn- kw-enum [kws]
  (s/spec (s/and (s/conformer keyword) kws) :gen #(s/gen kws)))

(defn- kw [kw]
  (kw-enum #{kw}))

(defmulti arrow-type-spec (comp keyword :name))

(s/def :arrow/type (s/multi-spec arrow-type-spec :name))

(s/def :arrow.type.null/name (kw :null))

(defmethod arrow-type-spec :null [_]
  (s/keys :req-un [:arrow.type.null/name]))

(s/def :arrow.type.int/name (kw :int))
(s/def :arrow.type.int/bitWidth #{Byte/SIZE Short/SIZE Integer/SIZE Long/SIZE})
(s/def :arrow.type.int/isSigned boolean?)

(defmethod arrow-type-spec :int [_]
  (s/keys :req-un [:arrow.type.int/name
                   :arrow.type.int/bitWidth]
          :opt-un [:arrow.type.int/isSigned]))

(s/def :arrow.type.floatingpoint/name (kw :floatingpoint))
(s/def :arrow.type.floatingpoint/precision (kw-enum #{:HALF :SINGLE :DOUBLE}))

(defmethod arrow-type-spec :floatingpoint [_]
  (s/keys :req-un [:arrow.type.floatingpoint/name
                   :arrow.type.floatingpoint/precision]))

(s/def :arrow.type.binary/name (kw :binary))

(defmethod arrow-type-spec :binary [_]
  (s/keys :req-un [:arrow.type.binary/name]))

(s/def :arrow.type.utf8/name (kw :utf8))

(defmethod arrow-type-spec :utf8 [_]
  (s/keys :req-un [:arrow.type.utf8/name]))

(s/def :arrow.type.bool/name (kw :bool))

(defmethod arrow-type-spec :bool [_]
  (s/keys :req-un [:arrow.type.bool/name]))

(s/def :arrow.type.decimal/name (kw :decimal))
(s/def :arrow.type.decimal/precision nat-int?)
(s/def :arrow.type.decimal/scale nat-int?)
(s/def :arrow.type.decimal/bitWidth #{128 256})

(defmethod arrow-type-spec :decimal [_]
  (s/keys :req-un [:arrow.type.decimal/name
                   :arrow.type.decimal/precision
                   :arrow.type.decimal/scale]
          :opt-un [:arrow.type.decimal/bitWidth]))

(s/def :arrow.type.date/name (kw :date))
(s/def :arrow.type.date/unit (kw-enum #{:DAY :MILLISECOND}))

(defmethod arrow-type-spec :date [_]
  (s/keys :req-un [:arrow.type.date/name]
          :opt-un [:arrow.type.date/unit]))

(s/def :arrow.type.time/name (kw :time))
(s/def :arrow.type.time/unit (kw-enum #{:SECOND :MILLISECOND :MICROSECOND :NANOSECOND}))
(s/def :arrow.type.time/bitWidth #{Integer/SIZE Long/SIZE})

(defmethod arrow-type-spec :time [_]
  (s/keys :req-un [:arrow.type.time/name]
          :opt-un [:arrow.type.time/unit
                   :arrow.type.time/bitWidth]))

(s/def :arrow.type.timestamp/name (kw :timestamp))
(s/def :arrow.type.timestamp/unit :arrow.type.time/unit)
(s/def :arrow.type.timestamp/timezone (s/spec (s/nilable string?) :gen #(s/gen (set (ZoneId/getAvailableZoneIds)))))

(defmethod arrow-type-spec :timestamp [_]
  (s/keys :req-un [:arrow.type.timestamp/name
                   :arrow.type.timestamp/unit
                   :arrow.type.timestamp/timezone]))

(s/def :arrow.type.interval/name (kw :interval))
(s/def :arrow.type.interval/unit (kw-enum #{:YEAR_MONTH :DAY_TIME}))

(defmethod arrow-type-spec :interval [_]
  (s/keys :req-un [:arrow.type.interval/name
                   :arrow.type.interval/unit]))

(s/def :arrow.type.list/name (kw :list))

(defmethod arrow-type-spec :list [_]
  (s/keys :req-un [:arrow.type.list/name]))

(s/def :arrow.type.struct/name (kw :struct))

(defmethod arrow-type-spec :struct [_]
  (s/keys :req-un [:arrow.type.struct/name]))

(s/def :arrow.type.union/name (kw :union))
(s/def :arrow.type.union/mode (kw-enum #{:SPARSE :DENSE}))
(s/def :arrow.type.union/typeIds (s/coll-of nat-int? :distinct true))

(defmethod arrow-type-spec :union [_]
  (s/keys :req-un [:arrow.type.union/name
                   :arrow.type.union/mode]
          :opt-un [:arrow.type.union/typeIds]))

(s/def :arrow.type.fixedsizebinary/name (kw :fixedsizebinary))
(s/def :arrow.type.fixedsizebinary/byteWidth nat-int?)

(defmethod arrow-type-spec :fixedsizebinary [_]
  (s/keys :req-un [:arrow.type.fixedsizebinary/name
                   :arrow.type.fixedsizebinary/byteWidth]))

(s/def :arrow.type.fixedsizelist/name (kw :fixedsizelist))
(s/def :arrow.type.fixedsizelist/listSize nat-int?)

(defmethod arrow-type-spec :fixedsizelist [_]
  (s/keys :req-un [:arrow.type.fixedsizelist/name
                   :arrow.type.fixedsizelist/listSize]))

(s/def :arrow.type.map/name (kw :map))
(s/def :arrow.type.map/keysSorted boolean?)

(defmethod arrow-type-spec :map [_]
  (s/keys :req-un [:arrow.type.map/name]
          :opt-un [:arrow.type.map/keysSorted]))

(s/def :arrow.type.duration/name (kw :duration))
(s/def :arrow.type.duration/unit :arrow.type.time/unit)

(defmethod arrow-type-spec :duration [_]
  (s/keys :req-un [:arrow.type.duration/name]
          :opt-un [:arrow.type.duration/unit]))

(s/def :arrow.type.largeutf8/name (kw :largeutf8))

(defmethod arrow-type-spec :largeutf8 [_]
  (s/keys :req-un [:arrow.type.largeutf8/name]))

(s/def :arrow.type.largebinary/name (kw :largebinary))

(defmethod arrow-type-spec :largebinary [_]
  (s/keys :req-un [:arrow.type.largebinary/name]))

(s/def :arrow.type.largelist/name (kw :largelist))

(defmethod arrow-type-spec :largelist [_]
  (s/keys :req-un [:arrow.type.largelist/name]))

(s/def :arrow/key-value (s/map-of string? string?))

(s/def :arrow.field/type :arrow/type)
(s/def :arrow.field/name string?)
(s/def :arrow.field/nullable boolean?)
(s/def :arrow.field/children (s/coll-of :arrow.schema/field))
(s/def :arrow.field/metadata :arrow/key-value)
(s/def :arrow/field (s/keys :req-un [:arrow.field/type]
                            :opt-on [:arrow.field/name :arrow.field/nullable :arrow.field/children :arrow.field/metadata]))

(s/def :arrow.schema/fields (s/coll-of :arrow/field :min-count 1))
(s/def :arrow.schema/metadata :arrow/key-value)
(s/def :arrow/schema (s/keys :req-un [:arrow.schema/fields] :opt-un [:arrow.schema/metadata]))

(defmulti type-kind :name)

(defmethod type-kind :struct [_]
  :arrow.kind/struct)

(defmethod type-kind :map [_]
  :arrow.kind/map)

(defmethod type-kind :list [_]
  :arrow.kind/list)

(defmethod type-kind :fixedsizelist [_]
  :arrow.kind/fixedsizelist)

(defmethod type-kind :largelist [_]
  :arrow.kind/largelist)

(defmethod type-kind :union [_]
  :arrow.kind/union)

(defmethod type-kind :default [_]
  :arrow.kind/primitive)

(defn ->arrow-type ^org.apache.arrow.vector.types.pojo.ArrowType [arrow-type]
  (s/assert :arrow/type arrow-type)
  (.readValue arrow-type-reader (json/write-str arrow-type)))

(defn ->field ^org.apache.arrow.vector.types.pojo.Field [{:keys [name nullable type children metadata] :as field
                                                          :or {nullable true children []}}]
  (s/assert :arrow/field field)
  (Field. name (FieldType. nullable (->arrow-type type) nil metadata) (mapv ->field children)))

(defn ->schema ^org.apache.arrow.vector.types.pojo.Schema [{:keys [fields metadata] :as schema}]
  (s/assert :arrow/schema schema)
  (Schema. (mapv ->field fields) metadata))

(extend-protocol p/Datafiable
  ArrowType
  (datafy [x]
    (->> (json/read-str (.writeValueAsString object-writer x) :key-fn keyword)
         (s/conform :arrow/type)))

  Field
  (datafy [x]
    (let [children (.getChildren x)
          metadata (.getMetadata x)]
      (cond-> {:name (.getName x)
               :type (p/datafy (.getType x))
               :nullable (.isNullable x)}
        (seq children) (assoc :children (mapv p/datafy children))
        (seq metadata) (assoc :metadata metadata))))

  Schema
  (datafy [x]
    (let [metadata (.getCustomMetadata x)]
      (cond-> {:fields (mapv p/datafy (.getFields x))}
        (seq metadata) (assoc :metadata metadata)))))

(def ^:dynamic *merge-union-mode* :DENSE)

(defmulti merge-fields (fn [x y]
                         [(type-kind (:type x)) (type-kind (:type y))]))

(defmethod merge-fields [:arrow.kind/primitive :arrow.kind/primitive] [x y]
  (assert (= (:name x) (:name y)))
  (if (= (:type x) (:type y))
    (assoc x :nullable (boolean (or (:nullable x) (:nullable y))))
    (assoc x :type {:name :union :mode *merge-union-mode*} :children [x y])))

(defmethod merge-fields [:arrow.kind/union :arrow.kind/primitive] [x y]
  (assert (= (:name x) (:name y)))
  (if (some #{y} (:children x))
    x
    (update x :children conj y)))

(defmethod merge-fields [:arrow.kind/primitive :arrow.kind/union] [x y]
  (merge-fields y x))


;; TODO: this is too simplistic, needs to merge compatible kinds as well.
(defmethod merge-fields [:arrow.kind/union :arrow.kind/union] [x y]
  (assert (= (:name x) (:name y)))
  (if (= (get-in x [:type :mode]) (get-in y [:type :mode]))
    (update x :children (comp vec distinct concat) (:children y))
    (assoc x :type {:name :union :mode *merge-union-mode*} :children [x y])))

(defmethod merge-fields [:arrow.kind/list :arrow.kind/primitive] [x y]
  (assert (= (:name x) (:name y)))
  (assoc x :type {:name :union :mode *merge-union-mode*} :children [x y]))

(defmethod merge-fields [:arrow.kind/primitive :arrow.kind/list] [x y]
  (assert (= (:name x) (:name y)))
  (assoc x :type {:name :union :mode *merge-union-mode*} :children [x y]))

(defmethod merge-fields [:arrow.kind/union :arrow.kind/list] [x y]
  (assert (= (:name x) (:name y)))
  (if (some #{y} (:children x))
    x
    (update x :children conj y)))

(defmethod merge-fields [:arrow.kind/list :arrow.kind/union] [x y]
  (merge-fields y x))

(defmethod merge-fields [:arrow.kind/primitive :arrow.kind/list] [x y]
  (assert (= (:name x) (:name y)))
  (assoc x :type {:name :union :mode *merge-union-mode*} :children [x y]))

(defmethod merge-fields [:arrow.kind/list :arrow.kind/primitive] [x y]
  (assert (= (:name x) (:name y)))
  (assoc x :type {:name :union :mode *merge-union-mode*} :children [x y]))

(defmethod merge-fields [:arrow.kind/list :arrow.kind/list] [x y]
  (assert (= (:name x) (:name y)))
  (assert (= 1 (count (:children x))))
  (assert (= 1 (count (:children y))))
  (assoc x :children [(merge-fields (first (:children x)) (first (:children y)))]))
