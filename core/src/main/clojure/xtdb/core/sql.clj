(ns xtdb.core.sql
  (:require [xtdb.operator :as op]
            [xtdb.rewrite :as r]
            [xtdb.sql.analyze :as sem]
            [xtdb.sql.parser :as parser]
            [xtdb.sql.plan :as plan]
            [xtdb.vector.writer :as vw])
  (:import clojure.lang.MapEntry
           xtdb.operator.PreparedQuery
           java.lang.AutoCloseable
           java.util.HashMap
           org.apache.arrow.memory.BufferAllocator))

(defn compile-query
  ([query] (compile-query query {}))

  ([query query-opts]
   (binding [r/*memo* (HashMap.)
             plan/*opts* query-opts]
     (let [ast (-> (parser/parse query) parser/or-throw
                   (sem/analyze-query) sem/or-throw)]
       (-> ast
           (plan/plan-query query-opts)
           (vary-meta assoc :param-count (sem/param-count ast))
           #_(doto clojure.pprint/pprint))))))

(defn open-sql-query ^xtdb.IResultSet [^BufferAllocator allocator, wm-src, ^PreparedQuery pq,
                                       {:keys [basis default-tz default-all-app-time?] :as query-opts}]
  (let [^AutoCloseable
        params (vw/open-params allocator
                               (->> (:? query-opts)
                                    (into {} (map-indexed (fn [idx v]
                                                            (MapEntry/create (symbol (str "?_" idx)) v))))))]
    (try
      (-> (.bind pq wm-src {:params params, :basis basis, :default-tz default-tz :default-all-app-time? default-all-app-time?})
          (.openCursor)
          (op/cursor->result-set params))
      (catch Throwable t
        (.close params)
        (throw t)))))
