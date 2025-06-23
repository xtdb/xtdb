(ns xtdb.flight-sql-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.flight-sql]
            [xtdb.test-util :as tu]
            [xtdb.types :as types])
  (:import (org.apache.arrow.adbc.core AdbcConnection)
           org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver
           (org.apache.arrow.flight CallOption FlightClient FlightEndpoint FlightInfo Location)
           (org.apache.arrow.flight.sql FlightSqlClient)
           (org.apache.arrow.vector VectorSchemaRoot)
           org.apache.arrow.vector.types.pojo.Schema
           xtdb.api.FlightSqlServer
           xtdb.arrow.Relation))

(def ^:private ^:dynamic ^FlightSqlClient *client* nil)
(def ^:private ^:dynamic *conn* nil)
(def ^:private ^:dynamic ^AdbcConnection *adbc-conn*)

(t/use-fixtures :each
  tu/with-allocator
  (fn [f]
    (tu/with-opts {:flight-sql-server {}}
      f))

  tu/with-node

  (fn [f]
    (let [port (.getPort ^FlightSqlServer (.module tu/*node* FlightSqlServer))]
      (with-open [flight-client (-> (FlightClient/builder tu/*allocator* (Location/forGrpcInsecure "127.0.0.1" port))
                                    (.build))
                  client (FlightSqlClient. flight-client)

                  conn (jdbc/get-connection {:jdbcUrl (format "jdbc:arrow-flight-sql://localhost:%d?useEncryption=false" port)})
                  adbc-db (-> (FlightSqlDriver. tu/*allocator*)
                              (.open  {"uri" (str "grpc+tcp://127.0.0.1:" port)}))
                  adbc-conn (.connect adbc-db)]

        (binding [*client* client, *conn* conn, *adbc-conn* adbc-conn]
          (f))))))

(def ^:private ^"[Lorg.apache.arrow.flight.CallOption;"
  empty-call-opts
  (make-array CallOption 0))

(defn- flight-info->rows [^FlightInfo flight-info]
  (let [ticket (.getTicket ^FlightEndpoint (first (.getEndpoints flight-info)))]
    (with-open [stream (.getStream *client* ticket empty-call-opts)]
      (let [root (.getRoot stream)
            !res (atom [])]
        (with-open [rel (Relation/fromRoot tu/*allocator* root)]
          (while (.next stream)
            ;; if this were a real client chances are they wouldn't just
            ;; eagerly turn the roots into Clojure maps...
            (.loadFromArrow rel root)
            (swap! !res into (.toMaps rel))))

        @!res))))

(t/deftest test-client
  (t/is (= -1 (.executeUpdate *client* "INSERT INTO users (_id, name) VALUES ('jms', 'James'), ('hak', 'Håkan')" empty-call-opts)))

  (t/is (= #{{:xt/id "jms", :name "James"}
             {:xt/id "hak", :name "Håkan"}}
           (set (-> (.execute *client* "SELECT _id, name FROM users" empty-call-opts)
                    (flight-info->rows))))))

#_ ; FIXME upgrade from Arrow 14 -> 15 killed this one.
(t/deftest test-jdbc-client
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO users (_id, name) VALUES ('jms', 'James')"]])

  ;; NOTE FSQL JDBC driver doesn't seem happy with prepared statement updates
  ;; see https://issues.apache.org/jira/browse/ARROW-18294
  #_
  (with-open [ps (jdbc/prepare *conn* ["INSERT INTO users (_id, name) VALUES ('jms', 'James')"])]
    ;; NOTE: next.jdbc submits everything as just `execute` rather than `executeUpdate`
    ;; FSQL depends on this difference, so we call `.executeUpdate` directly
    (.executeUpdate ps))

  ;; and ideally we'd do this with params - but this fails with 'parameter index out of range'
  ;; despite us returning some parameter metadata on the prepared statement
  #_
  (with-open [ps (jdbc/prepare *conn* ["INSERT INTO users (_id, name) VALUES (?, ?)"])]
    (.executeUpdate ps)
    (jdbc-prep/set-parameters ps ["hak" "Håkan"])
    (.executeUpdate ps))

  #_ ; or batches
  (jdbc/execute-batch! *conn* "INSERT INTO users (_id, name) VALUES (?, ?)"
                       [["jms" "James"], ["hak" "Håkan"]] {})

  (t/is (= #{{:_id "jms", :name "James"}}
           (set (jdbc/execute! *conn* ["SELECT users._id, users.name FROM users"]))))

  (jdbc/with-transaction [tx *conn*]
    #_ ; FSQL JDBC doesn't seem happy with params in a query
    (t/is (= [] (jdbc/execute! tx ["SELECT users._id FROM users WHERE users.id = ?" "foo"])))

    (with-open [ps (jdbc/prepare tx ["SELECT users._id, users.name FROM users"])]
      (t/is (= #{{:_id "jms", :name "James"}} (set (jdbc/execute! ps)))))))

(t/deftest test-transaction
  (let [fsql-tx (.beginTransaction *client* empty-call-opts)]
    (t/is (= -1 (.executeUpdate *client* "INSERT INTO users (_id, name) VALUES ('jms', 'James'), ('hak', 'Håkan')" fsql-tx empty-call-opts)))
    (t/is (= []
             (-> (.execute *client* "SELECT users._id, users.name FROM users" empty-call-opts)
                 (flight-info->rows))))
    (.commit *client* fsql-tx empty-call-opts)

    (t/is (= #{{:xt/id "jms", :name "James"}
               {:xt/id "hak", :name "Håkan"}}
             (set (-> (.execute *client* "SELECT users._id, users.name FROM users" empty-call-opts)
                      (flight-info->rows)))))))

(t/deftest test-prepared-stmts
  (with-open [ps (.prepare *client* "INSERT INTO users (_id, name) VALUES (?, ?)" empty-call-opts)
              param-root (VectorSchemaRoot/create (Schema. [(types/col-type->field '_id :utf8) (types/col-type->field 'name :utf8)]) tu/*allocator*)]
    (.setParameters ps param-root)

    (tu/populate-root param-root [{:_id "jms", :name "James"}
                                  {:_id "mat", :name "Matt"}])
    (.executeUpdate ps empty-call-opts)

    (tu/populate-root param-root [{:_id "hak", :name "Håkan"}
                                  {:_id "wot", :name "Dan"}])
    (.executeUpdate ps empty-call-opts))

  (with-open [ps (.prepare *client* "SELECT users.name FROM users WHERE users._id >= ?" empty-call-opts)
              param-root (VectorSchemaRoot/create (Schema. [(types/col-type->field '$1 :utf8)]) tu/*allocator*)]
    (.setParameters ps param-root)

    (tu/populate-root param-root [{:$1 "l"}])
    (t/is (= #{{:name "Matt"}
               {:name "Dan"}}
             (set (-> (.execute ps empty-call-opts)
                      (flight-info->rows)))))

    (tu/populate-root param-root [{:$1 "j"}])
    (t/is (= #{{:name "James"}
               {:name "Matt"}
               {:name "Dan"}}
             (set (-> (.execute ps empty-call-opts)
                      (flight-info->rows)))))))

(t/deftest test-prepared-stmts-in-tx
  (letfn [(q []
            (set (-> (.execute *client* "SELECT users.name FROM users" empty-call-opts)
                     (flight-info->rows))))]
    (let [fsql-tx (.beginTransaction *client* empty-call-opts)]
      (with-open [ps (.prepare *client* "INSERT INTO users (_id, name) VALUES (?, ?)" fsql-tx empty-call-opts)
                  param-root (VectorSchemaRoot/create (Schema. [(types/col-type->field '_id :utf8) (types/col-type->field 'name :utf8)]) tu/*allocator*)]
        (.setParameters ps param-root)

        (tu/populate-root param-root [{:_id "jms", :name "James"}
                                      {:_id "mat", :name "Matt"}])
        (.executeUpdate ps empty-call-opts)

        (t/is (= #{} (q)))

        (tu/populate-root param-root [{:_id "hak", :name "Håkan"}
                                      {:_id "wot", :name "Dan"}])
        (.executeUpdate ps empty-call-opts)

        (t/is (= #{} (q))))

      (.commit *client* fsql-tx empty-call-opts)

      (t/is (= #{{:name "James"} {:name "Matt"} {:name "Håkan"} {:name "Dan"}}
               (q))))))

(t/deftest test-adbc
  (with-open [stmt (.createStatement *adbc-conn*)]
    (.setSqlQuery stmt "
       INSERT INTO foo RECORDS
       {
         -- https://xkcd.com/221/
         _id: UUID 'b82ae7b2-13cf-4828-858d-cd992fec9aa7',

         name: 'foo',
         created_at: TIMESTAMP '2020-01-01T12:34:00Z'
       }")
    (.executeUpdate stmt)

    (.setSqlQuery stmt "SELECT * FROM foo")

    (with-open [rdr (.getReader (.executeQuery stmt))]
      (with-open [root (.getVectorSchemaRoot rdr)]
        (t/is (true? (.loadNextBatch rdr)))

        (with-open [rel (Relation/fromRoot tu/*allocator* root)]
          (t/is (= [{:xt/id #uuid "b82ae7b2-13cf-4828-858d-cd992fec9aa7"
                     :name "foo"
                     :created-at #xt/zoned-date-time "2020-01-01T12:34Z"}]
                   (.toMaps rel))))

        (t/is (false? (.loadNextBatch rdr)))))))
