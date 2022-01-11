(ns core2.operator.csv
  (:require [clojure.data.csv :as csv]
            [clojure.instant :as inst]
            [core2.edn :as edn]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [core2.vector.writer :as vw])
  (:import core2.ICursor
           java.lang.AutoCloseable
           [java.nio.file Files Path]
           [java.util Base64 Iterator]
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.types.Types$MinorType
           org.apache.arrow.vector.VectorSchemaRoot))

(deftype CSVCursor [^BufferAllocator allocator
                    ^AutoCloseable rdr
                    ^VectorSchemaRoot root
                    col-parsers
                    ^Iterator row-batches]
  ICursor
  (getColumnNames [_] (util/root->col-names root))

  (tryAdvance [_ c]
    (if (.hasNext row-batches)
      (let [row-batch (.next row-batches)
            row-count (count row-batch)]
        (.clear root)

        (dorun
         (map-indexed (fn [col-idx fv]
                        (let [parse-value (nth col-parsers col-idx)
                              writer (vw/vec->writer fv)]
                          (dotimes [row-idx row-count]
                            (doto writer
                              (.startValue)
                              (->> (types/write-value! (-> (nth row-batch row-idx)
                                                           (nth col-idx)
                                                           parse-value)))
                              (.endValue)))))
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
   :duration edn/duration-reader})

(def ->arrow-type
  {:bigint (.getType Types$MinorType/BIGINT)
   :float8 (.getType Types$MinorType/FLOAT8)
   :varbinary (.getType Types$MinorType/VARBINARY)
   :varchar (.getType Types$MinorType/VARCHAR)
   :bit (.getType Types$MinorType/BIT)
   :timestamp types/timestamp-micro-tz-type
   :duration types/duration-micro-type})

(defn ^core2.ICursor ->csv-cursor
  ([^BufferAllocator allocator, ^Path path, col-types]
   (->csv-cursor allocator path col-types {}))

  ([^BufferAllocator allocator, ^Path path, col-types {:keys [batch-size], :or {batch-size 1000}}]
   (let [rdr (Files/newBufferedReader path)
         [col-names & rows] (csv/read-csv rdr)
         col-types (map #(get col-types % :varchar) col-names)
         schema (Schema. (map (fn [col-name col-type]
                                (types/->field col-name
                                               (->arrow-type col-type)
                                               false))
                              col-names col-types))]
     (CSVCursor. allocator rdr
                 (VectorSchemaRoot/create schema allocator)
                 (mapv col-parsers col-types)
                 (.iterator ^Iterable (partition-all batch-size rows))))))
