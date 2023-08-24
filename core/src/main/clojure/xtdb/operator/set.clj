(ns xtdb.operator.set
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err]
            [xtdb.expression.map :as emap]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (java.util LinkedList)
           java.util.function.Consumer
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           xtdb.ICursor
           xtdb.expression.map.IRelationMap
           (xtdb.vector RelationReader)))

(defmethod lp/ra-expr :distinct [_]
  (s/cat :op #{:δ :distinct}
         :relation ::lp/ra-expression))

(defmethod lp/ra-expr :intersect [_]
  (s/cat :op #{:∩ :intersect}
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :union-all [_]
  (s/cat :op #{:∪ :union-all}
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(defmethod lp/ra-expr :difference [_]
  (s/cat :op #{:− :except :difference}
         :left ::lp/ra-expression
         :right ::lp/ra-expression))

(s/def ::incremental? boolean?)

(defmethod lp/ra-expr :fixpoint [_]
  (s/cat :op #{:μ :mu :fixpoint}
         :mu-variable ::lp/relation
         :opts (s/? (s/keys :opt-un [::incremental?]))
         :base ::lp/ra-expression
         :recursive ::lp/ra-expression))

(defmethod lp/ra-expr :relation [_]
  (s/and ::lp/relation
         (s/conformer (fn [rel]
                        {:op :relation, :relation rel})
                      :relation)))

(defmethod lp/ra-expr :assign [_]
  (s/cat :op #{:← :assign}
         :bindings (s/and vector?
                          (s/* (s/cat :variable ::lp/relation,
                                      :value ::lp/ra-expression)))
         :relation ::lp/ra-expression))

(set! *unchecked-math* :warn-on-boxed)

(defn- union-col-types [left-col-types right-col-types]
  (when-not (= (set (keys left-col-types)) (set (keys right-col-types)))
    (throw (err/illegal-arg :union-incompatible-cols
                            {::err/message "union incompatible cols"
                             :left-col-names (set (keys left-col-types))
                             :right-col-names (set (keys right-col-types))})))

  ;; NOTE: this overestimates types for intersection - if one side's string and the other int,
  ;; they statically can't intersect - but maybe that's one step too far for now.
  (merge-with types/merge-col-types left-col-types right-col-types))

(deftype UnionAllCursor [^ICursor left-cursor
                         ^ICursor right-cursor]
  ICursor
  (tryAdvance [_ c]
    (let [advanced? (boolean-array 1 false)]
      (loop []
        (if (or (.tryAdvance left-cursor
                             (reify Consumer
                               (accept [_ in-rel]
                                 (let [^RelationReader in-rel in-rel]
                                   (when (pos? (.rowCount in-rel))
                                     (aset advanced? 0 true)
                                     (.accept c in-rel))))))
                (.tryAdvance right-cursor
                             (reify Consumer
                               (accept [_ in-rel]
                                 (let [^RelationReader in-rel in-rel]
                                   (when (pos? (.rowCount in-rel))
                                     (aset advanced? 0 true)
                                     (.accept c in-rel)))))))
          (if (aget advanced? 0)
            true
            (recur))
          false))))
  (close [_]
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defmethod lp/emit-expr :union-all [{:keys [left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
    (fn [left-col-types right-col-types]
      {:col-types (union-col-types left-col-types right-col-types)
       :->cursor (fn [_opts left-cursor right-cursor]
                   (UnionAllCursor. left-cursor right-cursor))})))

(deftype IntersectionCursor [^ICursor left-cursor
                             ^ICursor right-cursor
                             ^IRelationMap rel-map
                             difference?]
  ICursor
  (tryAdvance [_ c]
    (.forEachRemaining right-cursor
                       (reify Consumer
                         (accept [_ in-rel]
                           (let [^RelationReader in-rel in-rel
                                 builder (.buildFromRelation rel-map in-rel)]
                             (dotimes [idx (.rowCount in-rel)]
                               (.add builder idx))))))

    (boolean
     (let [advanced? (boolean-array 1)]
       (while (and (not (aget advanced? 0))
                   (.tryAdvance left-cursor
                                (reify Consumer
                                  (accept [_ in-rel]
                                    (let [^RelationReader in-rel in-rel
                                          row-count (.rowCount in-rel)
                                          prober (.probeFromRelation rel-map in-rel)]

                                      (when (pos? row-count)
                                        (let [idxs (IntStream/builder)]
                                          (dotimes [idx row-count]
                                            (when (cond-> (not= -1 (.indexOf prober idx true))
                                                    difference? not)
                                              (.add idxs idx)))

                                          (let [idxs (.toArray (.build idxs))]
                                            (when-not (empty? idxs)
                                              (aset advanced? 0 true)
                                              (.accept c (.select in-rel idxs))))))))))))
       (aget advanced? 0))))

  (close [_]
    (util/try-close rel-map)
    (util/try-close left-cursor)
    (util/try-close right-cursor)))

(defmethod lp/emit-expr :intersect [{:keys [left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
    (fn [left-col-types right-col-types]
      (let [col-types (union-col-types left-col-types right-col-types)]
        {:col-types col-types
         :->cursor (fn [{:keys [allocator]} left-cursor right-cursor]
                     (IntersectionCursor. left-cursor right-cursor
                                          (emap/->relation-map allocator {:build-col-types left-col-types
                                                                          :key-col-names (set (keys col-types))})
                                          false))}))))

(defmethod lp/emit-expr :difference [{:keys [left right]} args]
  (lp/binary-expr (lp/emit-expr left args) (lp/emit-expr right args)
    (fn [left-col-types right-col-types]
      (let [col-types (union-col-types left-col-types right-col-types)]
        {:col-types col-types
         :->cursor (fn [{:keys [allocator]} left-cursor right-cursor]
                     (IntersectionCursor. left-cursor right-cursor
                                          (emap/->relation-map allocator {:build-col-types left-col-types
                                                                          :key-col-names (set (keys col-types))})
                                          true))}))))

(deftype DistinctCursor [^ICursor in-cursor
                         ^IRelationMap rel-map]
  ICursor
  (tryAdvance [_ c]
    (let [advanced? (boolean-array 1)]
      (while (and (not (aget advanced? 0))
                  (.tryAdvance in-cursor
                               (reify Consumer
                                 (accept [_ in-rel]
                                   (let [^RelationReader in-rel in-rel
                                         row-count (.rowCount in-rel)]
                                     (when (pos? row-count)
                                       (let [builder (.buildFromRelation rel-map in-rel)
                                             idxs (IntStream/builder)]
                                         (dotimes [idx row-count]
                                           (when (neg? (.addIfNotPresent builder idx))
                                             (.add idxs idx)))

                                         (let [idxs (.toArray (.build idxs))]
                                           (when-not (empty? idxs)
                                             (aset advanced? 0 true)
                                             (.accept c (.select in-rel idxs))))))))))))
      (aget advanced? 0)))

  (close [_]
    (util/try-close rel-map)
    (util/try-close in-cursor)))

(defmethod lp/emit-expr :distinct [{:keys [relation]} args]
  (lp/unary-expr (lp/emit-expr relation args)
    (fn [inner-col-types]
      {:col-types inner-col-types
       :->cursor (fn [{:keys [allocator]} in-cursor]
                   (DistinctCursor. in-cursor (emap/->relation-map allocator {:build-col-types inner-col-types
                                                                              :key-col-names (set (keys inner-col-types))
                                                                              :nil-keys-equal? true})))})))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ICursorFactory
  (^xtdb.ICursor createCursor []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface IFixpointCursorFactory
  (^xtdb.ICursor createCursor [^xtdb.operator.set.ICursorFactory cursor-factory]))

;; https://core.ac.uk/download/pdf/11454271.pdf "Algebraic optimization of recursive queries"
;; http://webdam.inria.fr/Alice/pdfs/Chapter-14.pdf "Recursion and Negation"

(defn ->fixpoint-cursor-factory [rel]
  (reify ICursorFactory
    (createCursor [_]
      (let [rels-queue (LinkedList. [rel])]
        (reify
          ICursor
          (tryAdvance [_ c]
            (if-let [rel (.poll rels-queue)]
              (do
                (.accept c rel)
                true)
              false))

          (close [_]))))))

(deftype FixpointCursor [^BufferAllocator allocator
                         ^ICursor base-cursor
                         ^IFixpointCursorFactory recursive-cursor-factory
                         ^IRelationMap rel-map
                         ^boolean incremental?
                         ^:unsynchronized-mutable ^ints new-idxs
                         ^:unsynchronized-mutable ^ICursor recursive-cursor
                         ^:unsynchronized-mutable continue?]
  ICursor
  (tryAdvance [this c]
    (if-not (or continue? recursive-cursor)
      false

      (let [advanced? (boolean-array 1)
            inner-c (reify Consumer
                      (accept [_ in-rel]
                        (let [^RelationReader in-rel in-rel]
                          (when (pos? (.rowCount in-rel))
                            (let [rel-builder (.buildFromRelation rel-map in-rel)
                                  new-idxs (IntStream/builder)]
                              (dotimes [idx (.rowCount in-rel)]
                                (let [map-idx (.addIfNotPresent rel-builder idx)]
                                  (when (neg? map-idx)
                                    (.add new-idxs (emap/inserted-idx map-idx)))))

                              (let [new-idxs (.toArray (.build new-idxs))]
                                (when-not (empty? new-idxs)
                                  (.accept c (-> (.getBuiltRelation rel-map) (.select new-idxs)))
                                  (set! (.continue? this) true)
                                  (set! (.new-idxs this) new-idxs)
                                  (aset advanced? 0 true))))))))]

        (.tryAdvance base-cursor inner-c)

        (or (aget advanced? 0)
            (do
              (while (and (not (aget advanced? 0)) continue?)
                (when-let [recursive-cursor (or recursive-cursor
                                                (when continue?
                                                  (set! (.continue? this) false)
                                                  (let [cursor (.createCursor recursive-cursor-factory
                                                                              (->fixpoint-cursor-factory (cond-> (.getBuiltRelation rel-map)
                                                                                                           incremental? (.select new-idxs))))]
                                                    (set! (.recursive-cursor this) cursor)
                                                    cursor)))]


                  (while (and (not (aget advanced? 0))
                              (let [more? (.tryAdvance recursive-cursor inner-c)]
                                (when-not more?
                                  (util/try-close recursive-cursor)
                                  (set! (.recursive-cursor this) nil))
                                more?)))))
              (aget advanced? 0))))))

  (close [_]
    (util/try-close rel-map)
    (util/try-close recursive-cursor)
    (util/try-close base-cursor)))

(def ^:dynamic *relation-variable->col-types* {})
(def ^:dynamic *relation-variable->cursor-factory* {})

(defmethod lp/emit-expr :relation [{:keys [relation]} _opts]
  (let [col-types (*relation-variable->col-types* relation)]
    {:col-types col-types
     :->cursor (fn [_opts]
                 (let [^ICursorFactory cursor-factory (get *relation-variable->cursor-factory* relation)]
                   (assert cursor-factory (str "can't find " relation, (pr-str *relation-variable->cursor-factory*)))
                   (.createCursor cursor-factory)))}))

(defmethod lp/emit-expr :fixpoint [{:keys [mu-variable base recursive], {:keys [incremental?]} :opts} args]
  (let [{base-col-types :col-types, ->base-cursor :->cursor} (lp/emit-expr base args)
        {recursive-col-types :col-types, ->recursive-cursor :->cursor} (binding [*relation-variable->col-types* (-> *relation-variable->col-types*
                                                                                                                    (assoc mu-variable base-col-types))]
                                                                         (lp/emit-expr recursive args))

        ;; HACK I think `:col-types` needs to be a fixpoint as well?
        col-types (union-col-types base-col-types recursive-col-types)]
    {:col-types col-types
     :->cursor
     (fn [{:keys [allocator] :as opts}]
       (FixpointCursor. allocator
                        (->base-cursor opts)
                        (reify IFixpointCursorFactory
                          (createCursor [_ cursor-factory]
                            (binding [*relation-variable->cursor-factory* (-> *relation-variable->cursor-factory*
                                                                              (assoc mu-variable cursor-factory))]
                              (->recursive-cursor opts))))
                        (emap/->relation-map allocator {:build-col-types col-types
                                                        :key-col-names (set (keys col-types))})
                        (boolean incremental?)
                        #_new-idxs nil
                        #_recursive-cursor nil
                        #_continue? true))}))

(defmethod lp/emit-expr :assign [{:keys [bindings relation]} args]
  (let [{:keys [rel-var->col-types relations]} (->> bindings
                                                     (reduce (fn [{:keys [rel-var->col-types relations]} {:keys [variable value]}]
                                                               (binding [*relation-variable->col-types* rel-var->col-types]
                                                                 (let [{:keys [col-types ->cursor]} (lp/emit-expr value args)]
                                                                   {:rel-var->col-types (-> rel-var->col-types
                                                                                            (assoc variable col-types))

                                                                    :relations (conj relations {:variable variable, :->cursor ->cursor})})))
                                                             {:rel-var->col-types *relation-variable->col-types*
                                                              :relations []}))
        {:keys [col-types ->cursor]} (binding [*relation-variable->col-types* rel-var->col-types]
                                       (lp/emit-expr relation args))]
    {:col-types col-types
     :->cursor (fn [opts]
                 (let [rel-var->cursor-factory (->> relations
                                                    (reduce (fn [acc {:keys [variable ->cursor]}]
                                                              (-> acc
                                                                  (assoc variable (reify ICursorFactory
                                                                                    (createCursor [_]
                                                                                      (binding [*relation-variable->cursor-factory* acc]
                                                                                        (->cursor opts)))))))
                                                            *relation-variable->cursor-factory*))]
                   (binding [*relation-variable->cursor-factory* rel-var->cursor-factory]
                     (->cursor opts))))}))
