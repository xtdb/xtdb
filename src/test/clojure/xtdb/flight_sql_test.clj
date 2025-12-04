(ns xtdb.flight-sql-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu])
  (:import (org.apache.arrow.adbc.core AdbcConnection)
           org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver
           (org.apache.arrow.flight CallOption FlightClient FlightEndpoint FlightInfo Location)
           (org.apache.arrow.flight.sql FlightSqlClient)
           (org.apache.arrow.vector VectorLoader VectorSchemaRoot)
           org.apache.arrow.vector.types.pojo.Schema
           xtdb.api.Xtdb
           xtdb.arrow.Relation))

(def ^:private ^:dynamic ^FlightSqlClient *client* nil)
(def ^:private ^:dynamic ^AdbcConnection *adbc-conn* nil)

(t/use-fixtures :each
  tu/with-allocator
  tu/with-node

  (fn [f]
    (let [port (.getFlightSqlPort ^Xtdb tu/*node*)]
      (with-open [flight-client (-> (FlightClient/builder tu/*allocator* (Location/forGrpcInsecure "127.0.0.1" port))
                                    (.build))
                  client (FlightSqlClient. flight-client)
                  adbc-db (-> (FlightSqlDriver. tu/*allocator*)
                              (.open  {"uri" (str "grpc+tcp://127.0.0.1:" port)}))
                  adbc-conn (.connect adbc-db)]

        (binding [*client* client, *adbc-conn* adbc-conn]
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
            (swap! !res into (.getAsMaps rel))))

        @!res))))

(t/deftest test-client
  (t/is (= -1 (.executeUpdate *client* "INSERT INTO users (_id, name) VALUES ('jms', 'James'), ('hak', 'Håkan')" empty-call-opts)))

  (t/is (= #{{:xt/id "jms", :name "James"}
             {:xt/id "hak", :name "Håkan"}}
           (set (-> (.execute *client* "SELECT _id, name FROM users" empty-call-opts)
                    (flight-info->rows))))))

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

(defn populate-root ^org.apache.arrow.vector.VectorSchemaRoot [^VectorSchemaRoot root rows]
  (with-open [rel (Relation. tu/*allocator* (.getSchema root))]
    (doseq [row rows]
      (.writeRow rel row))

    (with-open [rb (.openArrowRecordBatch rel)]
      (let [ldr (VectorLoader. root)]
        (.load ldr rb)))))

(t/deftest test-prepared-stmts
  (with-open [ps (.prepare *client* "INSERT INTO users (_id, name) VALUES (?, ?)" empty-call-opts)
              param-root (VectorSchemaRoot/create (Schema. [#xt/field {"_id" :utf8} #xt/field {"name" :utf8}]) tu/*allocator*)]
    (.setParameters ps param-root)

    (populate-root param-root [{:_id "jms", :name "James"}
                               {:_id "mat", :name "Matt"}])
    (.executeUpdate ps empty-call-opts)

    (populate-root param-root [{:_id "hak", :name "Håkan"}
                               {:_id "wot", :name "Dan"}])
    (.executeUpdate ps empty-call-opts))

  (with-open [ps (.prepare *client* "SELECT users.name FROM users WHERE users._id >= ?" empty-call-opts)
              param-root (VectorSchemaRoot/create (Schema. [#xt/field {"$1" :utf8}]) tu/*allocator*)]
    (.setParameters ps param-root)

    (populate-root param-root [{:$1 "l"}])
    (t/is (= #{{:name "Matt"}
               {:name "Dan"}}
             (set (-> (.execute ps empty-call-opts)
                      (flight-info->rows)))))

    (populate-root param-root [{:$1 "j"}])
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
                  param-root (VectorSchemaRoot/create (Schema. [#xt/field {"_id" :utf8} #xt/field {"name" :utf8}]) tu/*allocator*)]
        (.setParameters ps param-root)

        (populate-root param-root [{:_id "jms", :name "James"}
                                   {:_id "mat", :name "Matt"}])
        (.executeUpdate ps empty-call-opts)

        (t/is (= #{} (q)))

        (populate-root param-root [{:_id "hak", :name "Håkan"}
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
                   (.getAsMaps rel))))

        (t/is (false? (.loadNextBatch rdr)))))))
