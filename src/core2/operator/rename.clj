(ns core2.operator.rename
  (:require [core2.util :as util])
  (:import core2.ICursor
           java.util.Map
           java.util.function.Consumer
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.VectorSchemaRoot
           org.apache.arrow.vector.types.pojo.Field))

(deftype RenameCursor [^BufferAllocator allocator
                       ^ICursor in-cursor
                       ^Map #_#_<String, String> rename-map
                       ^:unsynchronized-mutable ^VectorSchemaRoot out-root]
  ICursor
  (tryAdvance [this c]
    (when out-root
      (.close out-root))

    (if (.tryAdvance in-cursor
                     (reify Consumer
                       (accept [_ in-root]
                         (let [^VectorSchemaRoot in-root in-root
                               ^Iterable out-vecs (for [^Field field (.getFields (.getSchema in-root))
                                                        :let [in-vec (.getVector in-root field)
                                                              field-name (.getName field)]]
                                                    (-> (.getTransferPair in-vec (get rename-map field-name field-name) allocator)
                                                        (doto (.splitAndTransfer 0 (.getValueCount in-vec)))
                                                        (.getTo)))]
                           (set! (.out-root this) (VectorSchemaRoot. out-vecs))))))
      (do
        (.accept c out-root)
        true)
      false))

  (close [_]
    (util/try-close out-root)
    (util/try-close in-cursor)))

(defn ->rename-cursor [^BufferAllocator allocator, ^ICursor in-cursor, ^Map #_#_<String, String> rename-map]
  (RenameCursor. allocator in-cursor rename-map nil))
