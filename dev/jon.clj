(ns jon
  (:require [crux.fixtures :as f]
            [crux.fixtures.calcite :as cf :refer [query]])
  (:import java.sql.DriverManager))

(defn sql-q [^java.sql.Connection conn q]
  (with-open [stmt (.createStatement conn)
              rs (.executeQuery stmt q)]
    (->> rs resultset-seq (into []))))

(comment
  (f/transact! (user/crux-node) (f/people [{:crux.db/id :ivan :name "Ivan" :homeworld "Earth" :alive true}
                                           {:crux.db/id :malcolm :name "Malcolm" :homeworld "Mars" :alive false}]))

  (f/transact! (user/crux-node) [{:crux.db/id :crux.sql.schema/person
                                  :crux.sql.table/name "person"
                                  :crux.sql.table/query '{:find [?id ?name ?homeworld ?alive]
                                                          :where [[?id :name ?name]
                                                                  [?id :homeworld ?homeworld]
                                                                  [?id :alive ?alive]]}
                                  :crux.sql.table/columns {'?id :keyword, '?name :varchar, '?homeworld :varchar, '?alive :boolean}}])

  (def conn (DriverManager/getConnection "jdbc:avatica:remote:url=http://localhost:1501;serialization=protobuf"))
  (sql-q conn "SELECT PERSON.NAME FROM PERSON ORDER BY NAME ASC"))
