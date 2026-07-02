(ns xtdb.vector.writer
  (:import (java.util LinkedHashMap)
           (xtdb.arrow Relation RelationReader RelationWriter)))

(set! *unchecked-math* :warn-on-boxed)

(defn open-args ^xtdb.arrow.RelationReader [allocator & args-rows]
  (let [args-maps (for [args-row args-rows]
                    (->> (map-indexed (fn [idx v]
                                        (if (map-entry? v)
                                          [(symbol (str "?" (symbol (key v)))) (val v)]
                                          [(symbol (str "?_" idx)) v]))
                                      args-row)
                         (reduce (fn [^LinkedHashMap m [sym v]]
                                   (doto m (.put sym v)))
                                 (LinkedHashMap.))))]
    (Relation/openFromRows allocator args-maps)))

(def empty-args RelationReader/DUAL)

(defn append-rel [^RelationWriter dest-rel, ^RelationReader src-rel]
  (.append dest-rel src-rel))
