(ns xtdb.operator.rename
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [xtdb.logical-plan :as lp]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr])
  (:import (java.util LinkedList Map)
           (xtdb.arrow RelationReader VectorReader)
           xtdb.ICursor))

(s/def ::prefix ::lp/relation)
(s/def ::columns (s/map-of ::lp/column ::lp/column :conform-keys true))

(defmethod lp/ra-expr :rename [_]
  (s/cat :op #{:ρ :rho :rename}
         :opts (s/keys :opt-un [::prefix ::columns])
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(deftype RenameCursor [^ICursor in-cursor
                       ^Map #_#_<Symbol, Symbol> col-name-mapping]
  ICursor
  (getCursorType [_] "rename")
  (getChildCursors [_] [in-cursor])

  (tryAdvance [_ c]
    (.tryAdvance in-cursor
                 (fn [^RelationReader in-rel]
                   (let [out-cols (LinkedList.)]

                     (doseq [^VectorReader in-col in-rel
                             :let [col-name (str (get col-name-mapping (symbol (.getName in-col))))]]
                       (.add out-cols (.withName in-col col-name)))

                     (.accept c (vr/rel-reader out-cols (.getRowCount in-rel)))))))

  (close [_]
    (util/try-close in-cursor)))

(defmethod lp/emit-expr :rename [{:keys [opts relation]} args]
  (let [{:keys [prefix columns]} opts
        {->inner-cursor :->cursor, inner-vec-types :vec-types, :as emitted-child-relation} (lp/emit-expr relation args)
        col-name-mapping (->> (for [old-name (set (keys inner-vec-types))]
                                [old-name
                                 (cond-> (get columns old-name old-name)
                                   prefix (->> name (symbol (name prefix))))])
                              (into {}))
        col-name-reverse-mapping (set/map-invert col-name-mapping)
        out-vec-types (->> inner-vec-types
                           (into {} (map (fn [[k v]] [(col-name-mapping k) v])))) ]
    {:op :rename
     :children [emitted-child-relation]
     :explain {:prefix (some-> prefix str), :columns (some-> columns pr-str)}
     :vec-types out-vec-types
     :stats (:stats emitted-child-relation)
     :->cursor (fn [{:keys [explain-analyze? tracer query-span pushdowns] :as opts}]
                 (let [opts (update opts :pushdowns update-keys #(get col-name-reverse-mapping %))]
                   (cond-> (util/with-close-on-catch [in-cursor (->inner-cursor opts)]
                             (RenameCursor. in-cursor col-name-mapping))
                     (or explain-analyze? (and tracer query-span)) (ICursor/wrapTracing tracer query-span nil nil (some-> pushdowns (update-keys str))))))}))
