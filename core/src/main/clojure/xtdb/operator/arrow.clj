(ns xtdb.operator.arrow
  (:require [clojure.spec.alpha :as s]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.indirect :as iv])
  (:import xtdb.ICursor
           java.io.BufferedInputStream
           [java.nio.file CopyOption Files Path]
           java.nio.channels.SeekableByteChannel
           java.net.URL
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           [org.apache.arrow.vector.ipc ArrowReader ArrowFileReader ArrowStreamReader InvalidArrowFileException]
           org.apache.arrow.vector.types.pojo.Field))

(defmethod lp/ra-expr :arrow [_]
  (s/cat :op #{:arrow}
         :url ::util/url))

(deftype ArrowCursor [^ArrowReader rdr on-close-fn]
  ICursor
  (tryAdvance [_ c]
    (if (.loadNextBatch rdr)
      (do
        (.accept c (iv/<-root (.getVectorSchemaRoot rdr)))
        true)
      false))

  (close [_]
    (util/try-close rdr)
    (when on-close-fn
      (on-close-fn))))

;; HACK: detection of stream vs file IPC format.
(defn- path->arrow-reader [^SeekableByteChannel ch ^BufferAllocator allocator]
  (try
    (doto (ArrowFileReader. ch allocator)
      (.initialize))
    (catch InvalidArrowFileException _
      (ArrowStreamReader. (.position ch 0) allocator))))

;; HACK: not ideal that we have to open the file in the emitter just to get the col-types?
(defn- path->cursor [^Path path on-close-fn]
  {:col-types (with-open [al (RootAllocator.)
                          ^ArrowReader rdr (path->arrow-reader (util/->file-channel path) al)]
                (->> (.getFields (.getSchema (.getVectorSchemaRoot rdr)))
                     (into {} (map (juxt #(symbol (.getName ^Field %)) types/field->col-type)))))
   :->cursor (fn [{:keys [^BufferAllocator allocator]}]
               (ArrowCursor. (path->arrow-reader (util/->file-channel path) allocator) on-close-fn))})

(defmethod lp/emit-expr :arrow [{:keys [^URL url]} _args]
  ;; TODO: should we make it possible to disable local files?
  (if (= "file" (.getProtocol url))
    (path->cursor (util/->path (.toURI url)) nil)
    ;; HACK: downloading during emit if protocol isn't file.
    (let [tmp-path (util/->temp-file "arrow_operator" "download")
          ^"[Ljava.nio.file.CopyOption;" options (make-array CopyOption 0)]
      (with-open [in (BufferedInputStream. (.openStream url))]
        (Files/copy in tmp-path options))
      (path->cursor tmp-path #(util/delete-file tmp-path)))))
