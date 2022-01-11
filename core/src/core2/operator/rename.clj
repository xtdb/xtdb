(ns core2.operator.rename
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [core2.operator.scan :as scan]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import core2.ICursor
           [core2.vector IIndirectVector IIndirectRelation]
           [java.util LinkedList Map]
           java.util.function.Consumer))

(set! *unchecked-math* :warn-on-boxed)

(def ^:const ^String relation-prefix-delimiter "_")

(deftype RenameCursor [^ICursor in-cursor
                       ^Map #_#_<String, String> rename-map
                       ^String prefix]
  ICursor
  (getColumnNames [_]
    (->> (.getColumnNames in-cursor)
         (into #{}
               (map (fn [old-name]
                      (cond->> (get rename-map old-name old-name)
                        prefix (str prefix relation-prefix-delimiter)))))))

  (tryAdvance [_ c]
    (binding [scan/*column->pushdown-bloom* (let [prefix-pattern (re-pattern (str "^" prefix relation-prefix-delimiter))
                                                  invert-rename-map (set/map-invert rename-map)]
                                              (->> (for [[k v] scan/*column->pushdown-bloom*
                                                         :let [k (str/replace k prefix-pattern "")
                                                               new-field-name (get invert-rename-map k k)]]
                                                     [new-field-name v])
                                                   (into {})))]
      (.tryAdvance in-cursor
                   (reify Consumer
                     (accept [_ in-rel]
                       (let [^IIndirectRelation in-rel in-rel
                             out-cols (LinkedList.)]

                         (doseq [^IIndirectVector in-col in-rel
                                 :let [old-name (.getName in-col)
                                       col-name (cond->> (get rename-map old-name old-name)
                                                  prefix (str prefix relation-prefix-delimiter))]]
                           (.add out-cols (.withName in-col col-name)))

                         (.accept c (iv/->indirect-rel out-cols))))))))

  (close [_]
    (util/try-close in-cursor)))

(defn ->rename-cursor ^core2.ICursor [^ICursor in-cursor, ^Map #_#_<String, String> rename-map ^String prefix]
  (RenameCursor. in-cursor rename-map prefix))
