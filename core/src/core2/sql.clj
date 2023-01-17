(ns core2.sql
  (:require [core2.operator :as op]
            [core2.rewrite :as r]
            [core2.sql.analyze :as sem]
            [core2.sql.parser :as parser]
            [core2.sql.plan :as plan]
            [core2.vector.writer :as vw])
  (:import clojure.lang.MapEntry
           core2.operator.PreparedQuery
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

(defn prepare-sql [query query-opts]
  (op/prepare-ra (compile-query query (select-keys query-opts [:app-time-as-of-now? :default-tz :decorrelate?]))))

(defn open-sql-query ^core2.IResultSet [^BufferAllocator allocator, ^PreparedQuery pq, db, query-opts]
  (let [^AutoCloseable
        params (vw/open-params allocator
                               (->> (:? query-opts)
                                    (into {} (map-indexed (fn [idx v]
                                                            (MapEntry/create (symbol (str "?_" idx)) v))))))]
    (try
      (-> (.bind pq {:srcs {'$ db}, :params params
                     :current-time (get-in query-opts [:basis :current-time])
                     :default-tz (:default-tz query-opts)})
          (.openCursor)
          (op/cursor->result-set params))
      (catch Throwable t
        (.close params)
        (throw t)))))
