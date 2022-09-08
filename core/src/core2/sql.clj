(ns core2.sql
  (:require [core2.rewrite :as r]
            [core2.sql.analyze :as sem]
            [core2.sql.parser :as parser]
            [core2.sql.plan :as plan])
  (:import java.util.HashMap))

(defn compile-query
  ([query] (compile-query query {}))

  ([query query-opts]
   (binding [r/*memo* (HashMap.)
             plan/*opts* query-opts]
     (-> (parser/parse query) parser/or-throw
         (sem/analyze-query) sem/or-throw
         (plan/plan-query query-opts)
         #_(doto clojure.pprint/pprint)))))
