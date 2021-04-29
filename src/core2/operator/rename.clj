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
                       ^VectorSchemaRoot out-root
                       ^IChunkCursor in-cursor
                       ^Map #_#_<String, String> rename-map
                       ^String prefix]
  IChunkCursor
  (getSchema [_] out-schema)

  (tryAdvance [_ c]
    (.clear out-root)

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
                           (let [^VectorSchemaRoot in-root in-root]
                             (dotimes [vec-idx (count (.getFields out-schema))]
                               (doto (.makeTransferPair (.getVector in-root vec-idx)
                                                        (.getVector out-root vec-idx))
                                 (.transfer)))
                             (util/set-vector-schema-root-row-count out-root (.getRowCount in-root))))))
        (do
          (.accept c out-root)
          true)
        false)))

  (close [_]
    (util/try-close out-root)
    (util/try-close in-cursor)))

(defn ->rename-cursor ^core2.IChunkCursor [^BufferAllocator allocator, ^IChunkCursor in-cursor, ^Map #_#_<String, String> rename-map ^String prefix]
  (let [schema (Schema. (for [^Field field (.getFields (.getSchema in-cursor))]
                          (let [old-name (.getName field)
                                new-name (cond->> (get rename-map old-name old-name)
                                           prefix (str prefix relation-prefix-delimiter))]
                            (Field. new-name (.getFieldType field) (.getChildren field)))))]

    (RenameCursor. allocator schema
                   (VectorSchemaRoot/create schema allocator)
                   in-cursor rename-map prefix)))
