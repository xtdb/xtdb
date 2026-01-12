(ns xtdb.operator.arrow
  (:require [clojure.spec.alpha :as s]
            [xtdb.logical-plan :as lp]
            [xtdb.util :as util])
  (:import java.io.BufferedInputStream
           java.net.URL
           (java.nio.file CopyOption Files Path)
           (org.apache.arrow.memory BufferAllocator RootAllocator)
           org.apache.arrow.vector.types.pojo.Field
           (xtdb.arrow Relation Relation$ILoader VectorType)
           xtdb.ICursor))

(defmethod lp/ra-expr :arrow [_]
  (s/cat :op #{:arrow}
         :url ::util/url))

(deftype ArrowCursor [^Relation rel, ^Relation$ILoader loader, on-close-fn]
  ICursor
  (getCursorType [_] "arrow")
  (getChildCursors [_] [])

  (tryAdvance [_ c]
    (if (.loadNextPage loader rel)
      (do
        (.accept c rel)
        true)
      false))

  (close [_]
    (util/try-close rel)
    (util/try-close loader)
    (when on-close-fn
      (on-close-fn))))

;; HACK: detection of stream vs file IPC format.
(defn- path->loader ^xtdb.arrow.Relation$ILoader [^BufferAllocator allocator, ^Path path]
  (try
    (Relation/loader allocator path)
    (catch IllegalArgumentException _
      (Relation/streamLoader allocator path))))

;; HACK: not ideal that we have to open the file in the emitter just to get the fields?
(defn- path->cursor [^Path path on-close-fn]
  (let [fields (with-open [al (RootAllocator.)
                           loader (path->loader al path)]
                 (->> (.getFields (.getSchema loader))
                      (into {} (map (juxt #(symbol (.getName ^Field %)) identity)))))]
    {:op :arrow
     :children []
     :vec-types (update-vals fields #(VectorType/fromField ^Field %))
     :->cursor (fn [{:keys [^BufferAllocator allocator explain-analyze? tracer query-span]}]
                 (util/with-close-on-catch [loader (path->loader allocator path)
                                            rel (Relation. allocator (.getSchema loader))]
                   (cond-> (ArrowCursor. rel loader on-close-fn)
                     (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span))))}))

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
