(ns xtdb.expression.list
  (:require [xtdb.expression :as expr]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (clojure.lang IPersistentMap)
           (xtdb.arrow ListExpression RelationReader ListValueReader)))

(def vec-writer-sym (gensym 'vec_writer))

(def compile-list-expr
  (-> (fn [expr opts]
        (let [expr (expr/prepare-expr expr)
              {:keys [return-type continue] :as emitted-expr} (expr/codegen-expr expr opts)
              field (types/unnest-field (types/col-type->field return-type))]
          {:field field
           :->list-expr (eval `(fn [~(-> expr/schema-sym (expr/with-tag IPersistentMap))
                                    ~(-> expr/args-sym (expr/with-tag RelationReader))]
                                 (let [~@(expr/batch-bindings emitted-expr)
                                       ^ListValueReader lvr# ~(continue (fn [_t c]
                                                                          c))]
                                   (reify ListExpression
                                     (getSize [_#] (.size lvr#))
                                     (writeTo [_# ~vec-writer-sym start# len#]
                                       (dotimes [~expr/idx-sym len#]
                                         (.writeValue ~vec-writer-sym (.nth lvr# (+ start# ~expr/idx-sym)))))))))}))
      (util/lru-memoize) ; <<no-commit>>
      expr/wrap-zone-id-cache-buster))
