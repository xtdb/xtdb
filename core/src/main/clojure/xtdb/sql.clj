(ns xtdb.sql
  (:require [xtdb.sql.plan :as plan]
            [xtdb.util :as util]))

(def compile-query
  (-> (fn compile-query
        ([query] (compile-query query {}))

        ([query query-opts]
         (let [plan (plan/plan-statement query query-opts)]
           (-> plan
               (plan/->logical-plan)
               (vary-meta assoc :param-count (:param-count (meta plan)))
               #_ (doto clojure.pprint/pprint))))) ;; <<no-commit>>
      #_util/lru-memoize)) ;; <<no-commit>>
