(ns core2.operator.arrow
  (:require [core2.logical-plan :as lp]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import core2.ICursor
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           org.apache.arrow.vector.ipc.ArrowFileReader
           org.apache.arrow.vector.types.pojo.Field
           java.nio.file.Path))

(deftype ArrowCursor [^ArrowFileReader rdr]
  ICursor
  (tryAdvance [_ c]
    (if (.loadNextBatch rdr)
      (do
        (.accept c (iv/<-root (.getVectorSchemaRoot rdr)))
        true)
      false))

  (close [_]
    (util/try-close rdr)))

(defmethod lp/emit-expr :arrow [{:keys [^Path path]} _args]
  ;; FIXME this didn't ever work from the LP - needs `:col-names`
  {:col-names (with-open [al (RootAllocator.)
                          rdr (ArrowFileReader. (util/->file-channel path) al)]
                (->> (.getFields (.getSchema (.getVectorSchemaRoot rdr)))
                     (into #{} (map #(.getName ^Field %)))))
   :->cursor (fn [{:keys [^BufferAllocator allocator]}]
               (ArrowCursor. (ArrowFileReader. (util/->file-channel path) allocator)))})
