(ns xtdb.sql
  (:require [xtdb.sql.plan :as plan]
            [xtdb.util :as util]))

(def compile-query
  (-> (fn compile-query
        ([query] (compile-query query {}))

        ([query query-opts]
         (let [{:keys [col-syms] :as plan} (plan/plan-statement query query-opts)]
           (-> plan
               (plan/->logical-plan)
               (vary-meta
                assoc
                :warnings (:warnings (meta plan))
                :param-count (:param-count (meta plan))
                :ordered-outer-projection col-syms)
               #_(doto clojure.pprint/pprint))))) ;; <<no-commit>>
      util/lru-memoize)) ;; <<no-commit>>
