(ns core2.operator.rename
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [core2.logical-plan :as lp]
            [core2.operator.scan :as scan]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import core2.ICursor
           [core2.vector IIndirectRelation IIndirectVector]
           [java.util LinkedList Map]
           java.util.function.Consumer))

(defmethod lp/ra-expr :rename [_]
  (s/cat :op #{:ρ :rho :rename}
         :prefix (s/? ::lp/relation)
         :columns (s/? (s/map-of ::lp/column ::lp/column :conform-keys true))
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(def ^:const ^String relation-prefix-delimiter "_")

(deftype RenameCursor [^ICursor in-cursor
                       ^Map #_#_<Symbol, Symbol> col-name-mapping
                       ^Map #_#_<Symbol, Symbol> col-name-reverse-mapping]
  ICursor
  (tryAdvance [_ c]
    (binding [scan/*column->pushdown-bloom* (->> (for [[k v] scan/*column->pushdown-bloom*]
                                                   [(get col-name-reverse-mapping k) v])
                                                 (into {}))]
      (.tryAdvance in-cursor
                   (reify Consumer
                     (accept [_ in-rel]
                       (let [^IIndirectRelation in-rel in-rel
                             out-cols (LinkedList.)]

                         (doseq [^IIndirectVector in-col in-rel
                                 :let [col-name (name (get col-name-mapping (symbol (.getName in-col))))]]
                           (.add out-cols (.withName in-col col-name)))

                         (.accept c (iv/->indirect-rel out-cols))))))))

  (close [_]
    (util/try-close in-cursor)))

(defmethod lp/emit-expr :rename [{:keys [columns relation prefix]} args]
  (lp/unary-expr (lp/emit-expr relation args)
    (fn [col-types]
      (let [col-name-mapping (->> (for [old-name (set (keys col-types))]
                                    [old-name
                                     (cond-> (get columns old-name old-name)
                                       prefix (->> name (str prefix relation-prefix-delimiter) symbol))])
                                  (into {}))]
        {:col-types (->> col-types
                         (into {}
                               (map (juxt (comp col-name-mapping key) val))))
         :->cursor (fn [_opts in-cursor]
                     (RenameCursor. in-cursor col-name-mapping (set/map-invert col-name-mapping)))}))))
