(ns xtdb.expression.list
  (:require [xtdb.expression :as expr]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (clojure.lang IPersistentMap)
           (xtdb.arrow ListExpression ListValueReader RelationReader)))

(def vec-writer-sym (gensym 'vec_writer))

(def compile-list-expr
  (-> (fn [expr opts]
        (let [expr (expr/prepare-expr expr)
              {:keys [return-type continue] :as emitted-expr} (expr/codegen-expr expr opts)
              el-type (->> (.getLegs return-type) (keep types/unnest-type) (apply types/merge-types))
              lvr-sym (gensym 'lvr)]
          {:vec-type el-type
           :->list-expr (eval `(fn [~(-> expr/schema-sym (expr/with-tag IPersistentMap))
                                    ~(-> expr/args-sym (expr/with-tag RelationReader))]
                                 (let [~@(expr/batch-bindings emitted-expr)]
                                   ~(continue (fn [t c]
                                                (case (types/col-type-head t)
                                                  (:list :set :fixed-size-list)
                                                  `(let [~(expr/with-tag lvr-sym ListValueReader) ~c]
                                                     (reify ListExpression
                                                       (getSize [_#] (.size ~lvr-sym))
                                                       (writeTo [_# ~vec-writer-sym start# len#]
                                                         (dotimes [~expr/idx-sym len#]
                                                           (.writeValue ~vec-writer-sym (.nth ~lvr-sym (+ start# ~expr/idx-sym)))))))

                                                  nil))))))}))
      (util/lru-memoize) ; <<no-commit>>
      expr/wrap-zone-id-cache-buster))
