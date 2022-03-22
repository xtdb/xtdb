(ns core2.sql.tpch-sql-test
  (:require [core2.sql :as sql]
            [instaparse.core :as insta]
            [clojure.java.io :as io]
            [clojure.test :as t]))

(t/deftest test-parse-tpch-queries
  (doseq [q (range 22)
          :let [f (format "q%02d.sql" (inc q))
                sql-ast (sql/parse (slurp (io/resource (str "core2/sql/tpch/" f))))]]
    (t/is (not (insta/failure? sql-ast))
          (str f ": " (pr-str (insta/get-failure sql-ast))))))
