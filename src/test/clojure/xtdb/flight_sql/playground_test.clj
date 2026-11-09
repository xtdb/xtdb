(ns xtdb.flight-sql.playground-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.flight-sql :as fsql]
            [xtdb.pgwire :as pgw]
            [xtdb.test-util :as tu])
  (:import (org.apache.arrow.flight CallOption FlightCallHeaders FlightClient FlightEndpoint FlightInfo HeaderCallOption Location)
           (org.apache.arrow.flight.client ClientCookieMiddleware$Factory)
           (org.apache.arrow.flight.sql FlightSqlClient)
           xtdb.api.FlightSql
           xtdb.arrow.Relation))

(def ^:private ^:dynamic *port* nil)

(t/use-fixtures :each
  tu/with-allocator

  (fn [f]
    (with-open [playground (pgw/open-playground)]
      (binding [*port* (.getPort ^FlightSql (:flight-sql playground))]
        (f)))))

(deftest catalog-option-creates-the-database
  (with-open [new-db-conn (fsql/open-conn {:port *port*, :dbname "newdb"})
              xtdb-conn (fsql/open-conn {:port *port*})]
    (xt/execute-tx new-db-conn [[:put-docs :docs {:xt/id "newdb"}]])
    (xt/execute-tx xtdb-conn [[:put-docs :docs {:xt/id "xtdb"}]])

    (t/is (= [{:xt/id "newdb"}] (xt/q new-db-conn "SELECT _id FROM docs")))
    (t/is (= [{:xt/id "xtdb"}] (xt/q xtdb-conn "SELECT _id FROM docs")))))

(defn- rows [^FlightSqlClient client ^FlightInfo info]
  (with-open [stream (.getStream client (.getTicket ^FlightEndpoint (first (.getEndpoints info))) (make-array CallOption 0))
              rel (Relation/fromRoot tu/*allocator* (.getRoot stream))]
    (loop [res []]
      (if (.next stream)
        (do
          (.loadFromArrow rel (.getRoot stream))
          (recur (into res (.getAsMaps rel))))
        res))))

(deftest database-header-creates-the-database
  (with-open [flight-client (-> (FlightClient/builder tu/*allocator* (Location/forGrpcInsecure "127.0.0.1" (int *port*)))
                                (.intercept (ClientCookieMiddleware$Factory.))
                                (.build))
              client (FlightSqlClient. flight-client)]
    (let [new-db-opts (into-array CallOption [(HeaderCallOption. (doto (FlightCallHeaders.)
                                                                    (.insert "x-xtdb-database" "newdb")))])]
      (.executeUpdate client "INSERT INTO docs RECORDS {_id: 'newdb'}" new-db-opts)

      (t/is (= [{:xt/id "newdb"}]
               (rows client (.execute client "SELECT _id FROM docs" new-db-opts)))))))
