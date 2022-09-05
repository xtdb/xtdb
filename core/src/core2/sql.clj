(ns core2.sql
  (:require [core2.sql.parser :as parser]
            [core2.sql.plan :as plan]))

(defn compile-query
  ([query] (compile-query query {}))

  ([query query-opts]
   (-> (parser/parse query) parser/or-throw
       (plan/plan-query query-opts) plan/or-throw)))
