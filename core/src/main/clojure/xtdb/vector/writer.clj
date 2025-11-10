(ns xtdb.vector.writer
  (:import (xtdb.arrow Relation RelationReader RelationWriter)))

(set! *unchecked-math* :warn-on-boxed)

(defn open-args ^xtdb.arrow.RelationReader [allocator args]
  (let [args-map (->> args
                      (into {} (map-indexed (fn [idx v]
                                              (if (map-entry? v)
                                                {(symbol (str "?" (symbol (key v)))) (val v)}
                                                {(symbol (str "?_" idx)) v})))))]

    (Relation/openFromRows allocator [args-map])))

(def empty-args RelationReader/DUAL)

(defn append-rel [^RelationWriter dest-rel, ^RelationReader src-rel]
  (.append dest-rel src-rel))
