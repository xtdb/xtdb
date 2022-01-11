(ns core2.operator.arrow
  (:require [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import core2.ICursor
           java.nio.file.Path
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.ipc.ArrowFileReader))

(deftype ArrowCursor [^ArrowFileReader rdr]
  ICursor
  (getColumnNames [_] (util/root->col-names (.getVectorSchemaRoot rdr)))

  (tryAdvance [_ c]
    (if (.loadNextBatch rdr)
      (do
        (.accept c (iv/<-root (.getVectorSchemaRoot rdr)))
        true)
      false))

  (close [_]
    (util/try-close rdr)))

(defn ->arrow-cursor ^core2.ICursor [^BufferAllocator allocator, ^Path path]
  ;; TODO might be worth checking that the Schema's something we can deal with
  (ArrowCursor. (ArrowFileReader. (util/->file-channel path) allocator)))
