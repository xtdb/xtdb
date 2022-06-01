(ns core2.operator.csv
  (:require [clojure.data.csv :as csv]
            [clojure.instant :as inst]
            [clojure.spec.alpha :as s]
            [core2.logical-plan :as lp]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw]
            [time-literals.data-readers :as time-literals.dr])
  (:import core2.ICursor
           java.lang.AutoCloseable
           [java.nio.file Files]
           [java.util Base64 Iterator]
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector ValueVector VectorSchemaRoot]
           org.apache.arrow.vector.types.pojo.Schema))

(s/def ::csv-col-type #{:bit :bigint :float8 :varchar :varbinary :timestamp :duration-milli})

(s/def ::batch-size pos-int?)

(defmethod lp/ra-expr :csv [_]
  (s/cat :op #{:csv}
         :path ::util/path
         :col-types (s/? (s/map-of ::lp/column ::csv-col-type))
         :opts (s/? (s/keys :opt-un [::batch-size]))))

(deftype CSVCursor [^BufferAllocator allocator
                    ^AutoCloseable rdr
                    ^VectorSchemaRoot root
                    col-parsers
                    ^Iterator row-batches]
  ICursor
  (tryAdvance [_ c]
    (if (.hasNext row-batches)
      (let [row-batch (.next row-batches)
            row-count (count row-batch)]
        (.clear root)

        (dorun
         (map-indexed (fn [col-idx ^ValueVector fv]
                        (when-let [parse-value (get col-parsers (.getName fv))]
                          (let [writer (vw/vec->writer fv)]
                            (dotimes [row-idx row-count]
                              (doto writer
                                (.startValue)
                                (->> (types/write-value! (-> (nth row-batch row-idx)
                                                             (nth col-idx)
                                                             parse-value)))
                                (.endValue))))))
                      (.getFieldVectors root)))

        (util/set-vector-schema-root-row-count root row-count)

        (.accept c (iv/<-root root))
        true)
      false))

  (close [_]
    (util/try-close rdr)
    (util/try-close root)))

(def ^:private ^java.util.Base64$Decoder b64-decoder
  (Base64/getDecoder))

(def ^:private col-parsers
  {:null (constantly nil)
   :bigint #(Long/parseLong %)
   :float8 #(Double/parseDouble %)
   :varbinary #(.decode b64-decoder ^String %)
   :varchar identity
   :bit #(or (= "1" %) (= "true" %))
   :timestamp inst/read-instant-date
   :duration time-literals.dr/duration})

(def ->arrow-type
  {:null types/null-type
   :bigint types/bigint-type
   :float8 types/float8-type
   :varbinary types/varbinary-type
   :varchar types/varchar-type
   :bit types/bool-type
   :timestamp types/timestamp-micro-tz-type
   :duration types/duration-micro-type})

(defmethod lp/emit-expr :csv [{:keys [path col-types],
                               {:keys [batch-size], :or {batch-size 1000}} :opts}
                              _args]
  (let [col-names (into #{} (map name) (keys col-types))]
    {:col-names col-names
     :->cursor (fn [{:keys [allocator]}]
                 (let [rdr (Files/newBufferedReader path)
                       rows (rest (csv/read-csv rdr))
                       schema (Schema. (map (fn [[col-name col-type]]
                                              (types/->field (name col-name)
                                                             (->arrow-type col-type)
                                                             false))
                                            col-types))]
                   (CSVCursor. allocator rdr
                               (VectorSchemaRoot/create schema allocator)
                               (->> col-types (into {} (map (juxt (comp name key)
                                                                  (comp col-parsers val)))))
                               (.iterator ^Iterable (partition-all batch-size rows)))))}))
