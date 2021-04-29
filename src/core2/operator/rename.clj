(ns core2.operator.rename
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [core2.operator.scan :as scan]
            [core2.util :as util])
  (:import core2.IChunkCursor
           java.util.function.Consumer
           java.util.Map
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector.types.pojo Field Schema]
           org.apache.arrow.vector.VectorSchemaRoot))

(set! *unchecked-math* :warn-on-boxed)

(def ^:const ^String relation-prefix-delimiter "_")

(deftype RenameCursor [^BufferAllocator allocator
                       ^Schema out-schema
                       ^IChunkCursor in-cursor
                       ^Map #_#_<String, String> rename-map
                       ^String prefix
                       ^:unsynchronized-mutable ^VectorSchemaRoot out-root]
  IChunkCursor
  (getSchema [_] out-schema)

  (tryAdvance [this c]
    (when out-root
      (.close out-root))

    (binding [scan/*column->pushdown-bloom* (let [prefix-pattern (re-pattern (str "^" prefix relation-prefix-delimiter))
                                                  invert-rename-map (set/map-invert rename-map)]
                                              (->> (for [[k v] scan/*column->pushdown-bloom*
                                                         :let [k (str/replace k prefix-pattern "")
                                                               new-field-name (get invert-rename-map k k)]]
                                                     [new-field-name v])
                                                   (into {})))]
      (if (.tryAdvance in-cursor
                       (reify Consumer
                         (accept [_ in-root]
                           (let [^VectorSchemaRoot in-root in-root
                                 ^Iterable out-vecs (for [^Field field (.getFields (.getSchema in-root))
                                                          :let [in-vec (.getVector in-root field)
                                                                field-name (.getName field)
                                                                new-field-name (get rename-map field-name field-name)
                                                                new-field-name (if prefix
                                                                                 (str prefix relation-prefix-delimiter new-field-name)
                                                                                 new-field-name)]]
                                                      (-> (.getTransferPair in-vec new-field-name allocator)
                                                          (doto (.splitAndTransfer 0 (.getValueCount in-vec)))
                                                          (.getTo)))]
                             (set! (.out-root this) (VectorSchemaRoot. out-vecs))))))
        (do
          (.accept c out-root)
          true)
        false)))

  (close [_]
    (util/try-close out-root)
    (util/try-close in-cursor)))

(defn ->rename-cursor ^core2.IChunkCursor [^BufferAllocator allocator, ^IChunkCursor in-cursor, ^Map #_#_<String, String> rename-map ^String prefix]
  (RenameCursor. allocator
                 (Schema. (for [^Field field (.getFields (.getSchema in-cursor))]
                            (let [old-name (.getName field)
                                  new-name (cond->> (get rename-map old-name old-name)
                                             prefix (str prefix relation-prefix-delimiter))]
                              (Field. new-name (.getFieldType field) (.getChildren field)))))
                 in-cursor rename-map prefix nil))
