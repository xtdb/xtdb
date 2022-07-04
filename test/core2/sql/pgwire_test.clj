(ns core2.sql.pgwire-test
  (:require [core2.sql.pgwire :as pgwire]
            [clojure.test :refer [deftest is testing] :as t]
            [core2.local-node :as node]
            [core2.test-util :as tu]
            [clojure.data.json :as json]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc])
  (:import (java.sql Connection)
           (org.postgresql.util PGobject)
           (com.fasterxml.jackson.databind.node JsonNodeType)
           (com.fasterxml.jackson.databind ObjectMapper JsonNode)))

(def ^:dynamic ^:private *port*)

(defn- each-fixture [f]
  (binding [*port* (tu/free-port)]
    (with-open [node (node/start-node {})
                _ (pgwire/->pg-wire-server node {:server-parameters {"server_version" "14"
                                                                     "server_encoding" "UTF8"
                                                                     "client_encoding" "UTF8"
                                                                     "TimeZone" "UTC"}
                                                 :port *port*
                                                 :num-threads 1})]
      (f))))

(t/use-fixtures :each each-fixture)

(deftest connect-test
  (with-open [_ (jdbc/get-connection (format "jdbc:postgresql://:%s/xtdb" *port*))]))

(defn- jdbc-conn ^Connection []
  (assert *port* "*port* must be bound")
  (jdbc/get-connection (format "jdbc:postgresql://:%s/xtdb" *port*)))

(deftest query-test
  (with-open [conn (jdbc-conn)]
    (with-open [stmt (.createStatement conn)
                rs (.executeQuery stmt "SELECT a.a FROM (VALUES ('hello, world')) a (a)")]
      (is (= true (.next rs)))
      (is (= false (.next rs))))))

(deftest prepared-query-test
  (with-open [conn (jdbc-conn)]
    (with-open [stmt (.prepareStatement conn "SELECT a.a FROM (VALUES ('hello, world')) a (a)")
                rs (.executeQuery stmt)]
      (is (= true (.next rs)))
      (is (= false (.next rs))))))

(def json-representation-examples
  "A map of entries describing sql value domains
  and properties of their json representation.

  :sql the SQL expression that produces the value
  :json-type the expected Jackson JsonNodeType

  :json (optional) a json string that we expect back from pgwire
  :clj (optional) a clj value that we expect from clojure.data.json/read-str
  :clj-pred (optional) a fn that returns true if the parsed arg (via data.json/read-str) is what we expect"
  (letfn [(string [s]
            {:sql (str "'" s "'")
             :json-type JsonNodeType/STRING
             :clj s})
          (integer [i]
            {:sql (str i)
             :json-type JsonNodeType/NUMBER
             :clj-pred #(= (bigint %) (bigint i))})
          (decimal [n]
            {:sql (.toPlainString (bigdec n))
             :json-type JsonNodeType/NUMBER
             :json (.toPlainString (bigdec n))
             :clj-pred #(= (bigdec %) (bigdec n))})]

    [{:sql "null"
      :json-type JsonNodeType/NULL
      :clj nil}

     {:sql "true"
      :json-type JsonNodeType/BOOLEAN
      :clj true}

     (string "hello, world")
     (string "")
     (string "42")
     (string "2022-01-03")

     ;; numbers
     ;

     (integer 0)
     (integer -0)
     (integer 42)
     (integer Long/MAX_VALUE)

     ;; does not work
     ;; MAX 9223372036854775807
     ;; MIN -9223372036854775808
     ;; min > max if you remove the sign
     #_(integer Long/MIN_VALUE)

     (decimal 0.0)
     (decimal -0.0)
     (decimal 3.14)
     (decimal 42.0)

     ;; does not work no exact decimal support currently
     #_(decimal Double/MIN_VALUE)
     #_(decimal Double/MAX_VALUE)

     ;; dates / times
     ;

     {:sql "DATE '2021-12-24'"
      :json-type JsonNodeType/STRING
      :clj "2021-12-24"}

     ;; does not work, no timestamp literals
     #_
     {:sql "TIMESTAMP '2021-12-24 11:23:44.003'"
      :json-type JsonNodeType/STRING
      :clj "2021-12-24T11:23:44.003Z"}

     ;; does not work
     ;; java.lang.ClassCastException: class org.apache.arrow.vector.IntervalYearVector cannot be cast to class org.apache.arrow.vector.IntervalMonthDayNanoVector
     #_{:sql "1 YEAR"
        :json-type JsonNodeType/STRING
        :clj "P1Y"}
     #_
     {:sql "1 MONTH"
      :json-type JsonNodeType/STRING
      :clj "P1M"}

     ;; arrays
     ;

     ;; does not work (cannot parse empty array)
     #_
     {:sql "ARRAY []"
      :json-type JsonNodeType/ARRAY
      :clj []}

     {:sql "ARRAY [42]"
      :json-type JsonNodeType/ARRAY
      :clj [42]}

     {:sql "ARRAY ['2022-01-02']"
      :json-type JsonNodeType/ARRAY
      :json "[\"2022-01-02\"]"
      :clj ["2022-01-02"]}

     ;; issue #245
     #_
     {:sql "ARRAY [ARRAY ['42'], 42, '42']"
      :json-type JsonNodeType/ARRAY
      :clj [["42"] 42 "42"]}]))

(deftest json-representation-test
  (with-open [conn (jdbc-conn)]
    (doseq [{:keys [json-type, json, sql, clj, clj-pred] :as example} json-representation-examples]
      (testing (str "SQL expression " sql " should parse to " clj " (" (when json (str json ", ")) json-type ")")
        (with-open [stmt (.prepareStatement conn (format "SELECT a.a FROM (VALUES (%s)) a (a)" sql))]
          (with-open [rs (.executeQuery stmt)]
            ;; one row in result set
            (.next rs)

            (testing "record set contains expected object"
              (is (instance? PGobject (.getObject rs 1)))
              (is (= "json" (.getType ^PGobject (.getObject rs 1)))))

            (testing (str "json parses to " (str json-type))
              (let [obj-mapper (ObjectMapper.)
                    json-str (str (.getObject rs 1))
                    ^JsonNode read-value (.readValue obj-mapper json-str ^Class JsonNode)]
                ;; use strings to get a better report
                (is (= (str json-type) (str (.getNodeType read-value))))
                (when json
                  (is (= json json-str) "json string should be = to :json"))))

            (testing "json parses to expected clj value"
              (let [clj-value (json/read-str (str (.getObject rs 1)))]
                (when (contains? example :clj)
                  (is (= clj clj-value) "parsed value should = :clj"))
                (when clj-pred
                  (is (clj-pred clj-value) "parsed value should pass :clj-pred"))))))))))
