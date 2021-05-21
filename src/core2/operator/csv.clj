(ns core2.operator.csv
  (:require [clojure.data.csv :as csv]
            [clojure.instant :as inst]
            [core2.relation :as rel]
            [core2.types :as types]
            [core2.util :as util])
  (:import core2.ICursor
           java.lang.AutoCloseable
           [java.nio.file Files Path]
           java.time.Duration
           [java.util Base64 Date Iterator]
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.util.Text
           org.apache.arrow.vector.VectorSchemaRoot))

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
        (util/set-vector-schema-root-row-count root row-count)

        (dorun
         (map-indexed (fn [col-idx fv]
                        (let [parse-value (nth col-parsers col-idx)]
                          (dotimes [row-idx row-count]
                            (types/set-safe! fv row-idx (-> (nth row-batch row-idx)
                                                            (nth col-idx)
                                                            parse-value)))))
                      (.getFieldVectors root)))
        (.accept c (rel/<-root root))
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
   :varchar #(Text. ^String %)
   :bit #(or (= "1" %) (= "true" %))
   :timestampmilli #(.getTime ^Date (inst/read-instant-date %))
   :durationmilli #(.toMillis (Duration/parse %))})

(defn ^core2.ICursor ->csv-cursor
  ([^BufferAllocator allocator, ^Path path, col-types]
   (->csv-cursor allocator path col-types {}))

  ([^BufferAllocator allocator, ^Path path, col-types {:keys [batch-size], :or {batch-size 1000}}]
   (let [rdr (Files/newBufferedReader path)
         [col-names & rows] (csv/read-csv rdr)
         col-types (map #(get col-types % :varchar) col-names)
         schema (Schema. (map (fn [col-name col-type]
                                (types/->field col-name
                                               (types/primitive-type->arrow-type col-type)
                                               false))
                              col-names col-types))]
     (CSVCursor. allocator rdr
                 (VectorSchemaRoot/create schema allocator)
                 (mapv col-parsers col-types)
                 (.iterator ^Iterable (partition-all batch-size rows))))))
