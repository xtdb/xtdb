(ns xtdb.operator.csv
  (:require [clojure.data.csv :as csv]
            [clojure.instant :as inst]
            [clojure.spec.alpha :as s]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import java.lang.AutoCloseable
           (java.nio.file Files)
           java.time.Duration
           (java.util Base64 Iterator)
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.types.pojo.Schema
           (xtdb.arrow Relation VectorWriter)
           xtdb.ICursor))

(s/def ::csv-col-type #{:bool :i64 :f64 :utf8 :varbinary :timestamp :duration})

(s/def ::batch-size pos-int?)

(defmethod lp/ra-expr :csv [_]
  (s/cat :op #{:csv}
         :path ::util/path
         :col-types (s/? (s/map-of ::lp/column ::csv-col-type))
         :opts (s/? (s/keys :opt-un [::batch-size]))))

(deftype CSVCursor [^BufferAllocator allocator
                    ^AutoCloseable rdr
                    ^Relation rel
                    col-parsers
                    ^Iterator row-batches]
  ICursor
  (getCursorType [_] "csv")
  (getChildCursors [_] [])

  (tryAdvance [_ c]
    (if (.hasNext row-batches)
      (let [row-batch (.next row-batches)
            row-count (count row-batch)]
        (.clear rel)

        (dorun
         (map-indexed (fn [col-idx ^VectorWriter v]
                        (when-let [parse-value (get col-parsers (.getName v))]
                          (dotimes [row-idx row-count]
                            (.writeObject v
                                          (-> (nth row-batch row-idx)
                                              (nth col-idx)
                                              parse-value)))))
                      (.getVectors rel)))

        (.setRowCount rel row-count)
        (.accept c rel)
        true)
      false))

  (close [_]
    (util/try-close rdr)
    (util/try-close rel)))

(def ^:private ^java.util.Base64$Decoder b64-decoder
  (Base64/getDecoder))

(def ^:private col-parsers
  (comp {:null (constantly nil)
         :i64 #(Long/parseLong %)
         :f64 #(Double/parseDouble %)
         :varbinary #(.decode b64-decoder ^String %)
         :utf8 identity
         :bool #(or (= "1" %) (= "true" %))
         :timestamp-tz inst/read-instant-date
         :duration #(Duration/parse %)}
        types/col-type-head))

(def ^:private csv-col-type-overrides
  {:timestamp [:timestamp-tz :micro "UTC"]
   :duration [:duration :micro]})

(defmethod lp/emit-expr :csv [{:keys [path col-types],
                               {:keys [batch-size], :or {batch-size 1000}} :opts}
                              _args]
  (let [fields (->> col-types
                    (into {} (map (juxt (comp name key)
                                        #(types/col-type->field (name (key %)) (get csv-col-type-overrides (val %) (val %)))))))]
    {:op :csv
     :children []
     :fields fields
     :->cursor (fn [{:keys [^BufferAllocator allocator, explain-analyze?]}]
                 (cond-> (let [rdr (Files/newBufferedReader path)
                               rows (rest (csv/read-csv rdr))
                               schema (Schema. (vals fields))]
                           (CSVCursor. allocator rdr
                                       (Relation. allocator schema)
                                       (->> fields (into {} (map (juxt (comp name key)
                                                                       (comp col-parsers types/field->col-type val)))))
                                       (.iterator ^Iterable (partition-all batch-size rows))))
                   explain-analyze? (ICursor/wrapExplainAnalyze)))}))
