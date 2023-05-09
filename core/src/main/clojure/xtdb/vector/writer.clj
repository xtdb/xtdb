(ns xtdb.vector.writer
  (:require [clojure.set :as set]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.indirect :as iv])
  (:import (java.lang AutoCloseable)
           (java.util LinkedHashMap)
           (java.util.function Function)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb.vector IIndirectRelation IIndirectVector IRelationWriter IRowCopier IVectorWriter IWriterPosition)))

;; TODO move these to `vec` eventually - currently there'd be a cyclic dep between `vec` and `iv`.

(defn ->vec-writer
  (^xtdb.vector.IVectorWriter [^BufferAllocator allocator, col-name]
   (vec/->writer (-> (types/->field col-name types/dense-union-type false)
                     (.createVector allocator))))

  (^xtdb.vector.IVectorWriter [^BufferAllocator allocator, col-name, col-type]
   (vec/->writer (-> (types/col-type->field col-name col-type)
                     (.createVector allocator)))))

(defn ->rel-writer ^xtdb.vector.IRelationWriter [^BufferAllocator allocator]
  (let [writers (LinkedHashMap.)
        wp (IWriterPosition/build)]
    (reify IRelationWriter
      (writerPosition [_] wp)

      (endRow [_] (.getPositionAndIncrement wp))

      (writerForName [_ col-name]
        (.computeIfAbsent writers col-name
                          (reify Function
                            (apply [_ col-name]
                              (doto (->vec-writer allocator col-name)
                                (vec/populate-with-absents (.getPosition wp)))))))

      (writerForName [_ col-name col-type]
        (.computeIfAbsent writers col-name
                          (reify Function
                            (apply [_ col-name]
                              (let [pos (.getPosition wp)]
                                (if (pos? pos)
                                  (doto (->vec-writer allocator col-name (types/merge-col-types col-type :absent))
                                    (vec/populate-with-absents pos))
                                  (->vec-writer allocator col-name col-type)))))))

      (rowCopier [this in-rel]
        (let [copiers (vec (concat (for [^IIndirectVector in-vec in-rel]
                                     (.rowCopier in-vec (.writerForName this (.getName in-vec))))

                                   (for [absent-col-name (set/difference (set (keys writers))
                                                                         (into #{} (map #(.getName ^IIndirectVector %)) in-rel))
                                         :let [!writer (delay
                                                         (-> (.writerForName this absent-col-name)
                                                             (.writerForType :absent)))]]
                                     (reify IRowCopier
                                       (copyRow [_ _src-idx]
                                         (let [pos (.getPosition wp)]
                                           (.writeNull ^IVectorWriter @!writer nil)
                                           pos))))))]
          (reify IRowCopier
            (copyRow [_ src-idx]
              (let [pos (.getPositionAndIncrement wp)]
                (doseq [^IRowCopier copier copiers]
                  (.copyRow copier src-idx))
                pos)))))

      (iterator [_] (.iterator (.values writers)))

      (clear [this]
        (run! #(.clear ^IVectorWriter %) this))

      AutoCloseable
      (close [this]
        (run! util/try-close this)))))

(defn open-rel ^xtdb.vector.IIndirectRelation [vecs]
  (iv/->indirect-rel (map iv/->direct-vec vecs)))

(defn open-params ^xtdb.vector.IIndirectRelation [allocator params-map]
  (open-rel (for [[k v] params-map]
              (vec/open-vec allocator k [v]))))

(def empty-params (iv/->indirect-rel [] 1))

(defn vec-wtr->rdr ^xtdb.vector.IIndirectVector [^xtdb.vector.IVectorWriter w]
  (iv/->direct-vec (.getVector w)))

(defn rel-wtr->rdr ^xtdb.vector.IIndirectRelation [^xtdb.vector.IRelationWriter w]
  (iv/->indirect-rel (map vec-wtr->rdr w)
                     (.getPosition (.writerPosition w))))

(defn append-vec [^IVectorWriter vec-writer, ^IIndirectVector in-col]
  (let [row-copier (.rowCopier in-col vec-writer)]
    (dotimes [src-idx (.getValueCount in-col)]
      (.copyRow row-copier src-idx))))

(defn append-rel [^IRelationWriter dest-rel, ^IIndirectRelation src-rel]
  (doseq [^IIndirectVector src-col src-rel
          :let [col-type (types/field->col-type (.getField (.getVector src-col)))
                ^IVectorWriter vec-writer (.writerForName dest-rel (.getName src-col) col-type)]]
    (append-vec vec-writer src-col))

  (let [wp (.writerPosition dest-rel)]
    (.setPosition wp (+ (.getPosition wp) (.rowCount src-rel)))))
