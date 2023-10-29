(ns xtdb.sql
  (:require [xtdb.operator :as op]
            [xtdb.rewrite :as r]
            [xtdb.sql.analyze :as sem]
            [xtdb.sql.parser :as parser]
            [xtdb.sql.plan :as plan]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import clojure.lang.MapEntry
           java.util.HashMap
           org.apache.arrow.memory.BufferAllocator
           xtdb.operator.PreparedQuery))

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

(defn open-sql-query ^xtdb.IResultSet [^BufferAllocator allocator, wm-src, ^PreparedQuery pq,
                                       {:keys [basis default-tz default-all-valid-time?] :as query-opts}]
  (util/with-close-on-catch [params (vw/open-params allocator
                                                    (->> (:args query-opts)
                                                         (into {} (map-indexed (fn [idx v]
                                                                                 (MapEntry/create (symbol (str "?_" idx)) v))))))]
    (-> (.bind pq wm-src {:params params, :basis basis, :default-tz default-tz :default-all-valid-time? default-all-valid-time?})
        (.openCursor)
        (op/cursor->result-set params))))
