(ns core2.core
  (:require [core2.types :as t]
            [core2.ingest :as ingest]
            [core2.log :as log]
            [clojure.string :as str]
            [core2.util :as util])
  (:import java.io.ByteArrayOutputStream
           java.nio.ByteBuffer
           java.nio.channels.Channels
           [java.nio.file Files OpenOption StandardOpenOption]
           java.nio.file.attribute.FileAttribute
           java.util.function.Function
           java.util.concurrent.CompletableFuture
           [org.apache.arrow.memory BufferAllocator]
           [org.apache.arrow.vector ValueVector VectorSchemaRoot]
           [org.apache.arrow.vector.complex DenseUnionVector StructVector]
           [org.apache.arrow.vector.ipc ArrowFileReader ArrowStreamWriter]
           [org.apache.arrow.vector.types Types$MinorType]
           [org.apache.arrow.vector.types.pojo Field Schema]
           [core2.ingest TransactionIngester TransactionInstant]
           [core2.object_store ObjectStore]
           [core2.log LogRecord LogReader LogWriter]))

(set! *unchecked-math* :warn-on-boxed)

(definterface SubmitTx
  (^java.util.concurrent.CompletableFuture submitTx [_ ]))

(def ^:private tx-arrow-schema
  (Schema. [(t/->field "tx-ops" (.getType Types$MinorType/DENSEUNION) false
                       (t/->field "put" (.getType Types$MinorType/STRUCT) false
                                  (t/->field "document" (.getType Types$MinorType/DENSEUNION) false))
                       (t/->field "delete" (.getType Types$MinorType/STRUCT) true))]))

(defn- add-element-to-union! [^DenseUnionVector duv type-id ^long parent-offset child-offset]
  (while (< (.getValueCapacity duv) (inc parent-offset))
    (.reAlloc duv))

  (.setTypeId duv parent-offset type-id)
  (.setInt (.getOffsetBuffer duv)
           (* DenseUnionVector/OFFSET_WIDTH parent-offset)
           child-offset))

(defn serialize-tx-ops ^java.nio.ByteBuffer [tx-ops ^BufferAllocator allocator]
  (with-open [root (VectorSchemaRoot/create tx-arrow-schema allocator)]
    (let [^DenseUnionVector tx-op-vec (.getVector root "tx-ops")

          union-type-ids (into {} (map vector
                                       (->> (.getChildren (.getField tx-op-vec))
                                            (map #(keyword (.getName ^Field %))))
                                       (range)))]

      (->> (map-indexed vector tx-ops)
           (reduce (fn [acc [op-idx {:keys [op] :as tx-op}]]
                     (let [^int per-op-offset (get-in acc [:per-op-offsets op] 0)
                           ^int op-type-id (get union-type-ids op)]

                       (add-element-to-union! tx-op-vec op-type-id op-idx per-op-offset)

                       (case op
                         :put (let [{:keys [doc]} tx-op

                                    ^StructVector put-vec (.getStruct tx-op-vec op-type-id)
                                    ^DenseUnionVector put-doc-vec (.getChild put-vec "document" DenseUnionVector)]

                                (.setIndexDefined put-vec per-op-offset)

                                ;; TODO we could/should key this just by the field name, and have a promotable union in the value.
                                ;; but, for now, it's keyed by both field name and type.
                                (let [doc-fields (->> (for [[k v] (sort-by key doc)]
                                                        [k (t/->field (name k) (t/->arrow-type (type v)) true)])
                                                      (into (sorted-map)))
                                      field-k (format "%08x" (hash doc-fields))
                                      ^Field doc-field (apply t/->field field-k (.getType Types$MinorType/STRUCT) true (vals doc-fields))
                                      field-type-id (or (->> (map-indexed vector (keys (get-in acc [:put :per-struct-offsets])))
                                                             (some (fn [[idx ^Field field]]
                                                                     (when (= doc-field field)
                                                                       idx))))
                                                        (.registerNewTypeId put-doc-vec doc-field))
                                      struct-vec (.getStruct put-doc-vec field-type-id)
                                      per-struct-offset (long (get-in acc [:put :per-struct-offsets doc-field] 0))]

                                  (.setIndexDefined struct-vec per-struct-offset)

                                  (add-element-to-union! put-doc-vec field-type-id per-op-offset per-struct-offset)

                                  (doseq [[k v] doc
                                          :let [^Field field (get doc-fields k)
                                                field-vec (.addOrGet struct-vec (name k) (.getFieldType field) ValueVector)]]
                                    (if (some? v)
                                      (t/set-safe! field-vec per-struct-offset v)
                                      (t/set-null! field-vec per-struct-offset)))

                                  (-> acc
                                      (assoc-in [:per-op-offsets :put] (inc per-op-offset))
                                      (assoc-in [:put :per-struct-offsets doc-field] (inc per-struct-offset))))))))
                   {}))

      (.setRowCount root (count tx-ops))
      (.syncSchema root)

      (with-open [baos (ByteArrayOutputStream.)
                  sw (ArrowStreamWriter. root nil (Channels/newChannel baos))]
        (doto sw
          (.start)
          (.writeBatch)
          (.end))
        (ByteBuffer/wrap (.toByteArray baos))))))

(defn log-record->tx-instant ^core2.ingest.TransactionInstant [^LogRecord record]
  (ingest/->TransactionInstant (.offset record) (.time record)))

(defn submit-tx ^java.util.concurrent.CompletableFuture [^LogWriter log-writer tx-ops ^BufferAllocator allocator]
  (.thenApply (.appendRecord log-writer (serialize-tx-ops tx-ops allocator))
              (reify Function
                (apply [_ result]
                  (log-record->tx-instant result)))))

(defn latest-completed-tx [^ObjectStore os, ^BufferAllocator allocator]
  (-> (.listObjects os)
      (.thenCompose (reify Function
                      (apply [_ ks]
                        (if-let [metadata-path (last (sort (filter #(str/starts-with? % "metadata-") ks)))]
                          (let [tmp-file (doto (Files/createTempFile metadata-path "" (make-array FileAttribute 0))
                                           (-> .toFile .deleteOnExit))]
                            (.getObject os metadata-path tmp-file))
                          (CompletableFuture/completedFuture nil)))))
      (.thenApply (reify Function
                    (apply [_ tmp-path]
                      (when tmp-path
                        (with-open [file-ch (Files/newByteChannel tmp-path (into-array OpenOption #{StandardOpenOption/READ}))
                                    file-reader (ArrowFileReader. file-ch allocator)]
                          (let [root (.getVectorSchemaRoot file-reader)]
                            (.loadNextBatch file-reader)
                            (let [field-vec (.getVector root "field")
                                  ->field-idx (fn [col-name]
                                                (reduce (fn [_ ^long idx]
                                                          (when (= (str (.getObject field-vec idx))
                                                                   col-name)
                                                            (reduced idx)))
                                                        nil
                                                        (range (.getValueCount field-vec))))
                                  tx-id-idx (->field-idx "_tx-id")
                                  tx-time-idx (->field-idx "_tx-time")

                                  max-vec (.getVector root "max")]
                              (ingest/->TransactionInstant (.getObject max-vec tx-id-idx)
                                                           (util/local-date-time->date (.getObject max-vec tx-time-idx))))))))))))

(defn index-all-available-transactions
  ([^LogReader log-reader
    ^TransactionIngester ingester
    ^TransactionInstant latest-completed-tx]
   (index-all-available-transactions log-reader ingester latest-completed-tx {}))

  ([^LogReader log-reader
    ^TransactionIngester ingester
    ^TransactionInstant latest-completed-tx
    {:keys [batch-size], :or {batch-size 100}}]
   (loop [latest-completed-tx latest-completed-tx]
     (if-let [log-records (seq (.readRecords log-reader (some-> latest-completed-tx .tx-id) batch-size))]
       (recur (reduce (fn [_ ^LogRecord record]
                        (let [tx-instant (log-record->tx-instant record)]
                          (.indexTx ingester tx-instant (.record record))
                          tx-instant))
                      nil
                      log-records))
       latest-completed-tx))))
