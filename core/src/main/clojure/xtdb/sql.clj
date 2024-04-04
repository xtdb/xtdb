(ns xtdb.sql
  (:require [xtdb.rewrite :as r]
            [xtdb.sql.analyze :as sem]
            [xtdb.sql.parser :as parser]
            [xtdb.sql.plan :as plan])
  (:import java.util.HashMap))

(defn parse-query
  [query]
  (binding [r/*memo* (HashMap.)]
    (-> (parser/parse query) parser/or-throw)))

(defn compile-query
  ([query] (compile-query query {}))

  ([query query-opts]
   (binding [r/*memo* (HashMap.)
             plan/*opts* query-opts
             sem/*table-info* (:table-info query-opts)]
     (let [ast (-> (parser/parse query) parser/or-throw
                   (sem/analyze-query) sem/or-throw)]
       (-> ast
           (plan/plan-query query-opts)
           (vary-meta assoc :param-count (sem/param-count ast))
           #_(doto clojure.pprint/pprint))))))
