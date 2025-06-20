(ns xtdb.operator.rename
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [xtdb.logical-plan :as lp]
            [xtdb.operator.scan :as scan]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (java.util LinkedList Map)
           (org.apache.arrow.vector.types.pojo Field)
           (xtdb.arrow RelationReader VectorReader)
           xtdb.ICursor))

(defmethod lp/ra-expr :rename [_]
  (s/cat :op #{:ρ :rho :rename}
         :prefix (s/? ::lp/relation)
         :columns (s/? (s/map-of ::lp/column ::lp/column :conform-keys true))
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(deftype RenameCursor [^ICursor in-cursor
                       ^Map #_#_<Symbol, Symbol> col-name-mapping
                       ^Map #_#_<Symbol, Symbol> col-name-reverse-mapping]
  ICursor
  (tryAdvance [_ c]
    (binding [scan/*column->pushdown-bloom* (->> (for [[k v] scan/*column->pushdown-bloom*]
                                                   [(get col-name-reverse-mapping k) v])
                                                 (into {}))]
      (.tryAdvance in-cursor
                   (fn [^RelationReader in-rel]
                     (let [out-cols (LinkedList.)]

                       (doseq [^VectorReader in-col in-rel
                               :let [col-name (str (get col-name-mapping (symbol (.getName in-col))))]]
                         (.add out-cols (.withName in-col col-name)))

                       (.accept c (vr/rel-reader out-cols (.getRowCount in-rel))))))))

  (close [_]
    (util/try-close in-cursor)))

(defmethod lp/emit-expr :rename [{:keys [columns relation prefix]} args]
  (let [{->inner-cursor :->cursor, inner-fields :fields, :as emitted-child-relation} (lp/emit-expr relation args)
        col-name-mapping (->> (for [old-name (set (keys inner-fields))]
                                [old-name
                                 (cond-> (get columns old-name old-name)
                                   prefix (->> name (symbol (name prefix))))])
                              (into {}))
        col-name-reverse-mapping (set/map-invert col-name-mapping)]
    {:fields (->> inner-fields
                  (into {}
                        (map (juxt (comp col-name-mapping key)
                                   (comp (fn [^Field field]
                                           (-> field
                                               (types/field-with-name (str (col-name-mapping (symbol (.getName field)))))))
                                         val)))))
     :stats (:stats emitted-child-relation)
     :->cursor (fn [opts]
                 (binding [scan/*column->pushdown-bloom* (->> (for [[k v] scan/*column->pushdown-bloom*]
                                                                [(get col-name-reverse-mapping k) v])
                                                              (into {}))]
                   (util/with-close-on-catch [in-cursor (->inner-cursor opts)]
                     (RenameCursor. in-cursor col-name-mapping col-name-reverse-mapping))))}))
