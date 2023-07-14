(ns xtdb.flight-sql-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.vector.reader :as vr]
            [juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc :as jdbc]
            #_[juxt.clojars-mirrors.nextjdbc.v1v2v674.next.jdbc.prepare :as jdbc-prep])
  (:import (org.apache.arrow.flight CallOption FlightClient FlightEndpoint FlightInfo Location)
           (org.apache.arrow.flight.sql FlightSqlClient)
           (org.apache.arrow.vector VectorSchemaRoot)
           org.apache.arrow.vector.types.pojo.Schema))

(def ^:private ^:dynamic *port*)
(def ^:private ^:dynamic ^FlightSqlClient *client*)
(def ^:private ^:dynamic *conn*)

(t/use-fixtures :each
  tu/with-allocator
  (fn [f]
    (binding [*port* (tu/free-port)]
      (tu/with-opts {:xtdb.flight-sql/server {:port *port*}}
        f)))

  tu/with-node

  (fn [f]
    (with-open [flight-client (-> (FlightClient/builder tu/*allocator* (Location/forGrpcInsecure "127.0.0.1" *port*))
                                  (.build))
                client (FlightSqlClient. flight-client)

                conn (jdbc/get-connection {:jdbcUrl (format "jdbc:arrow-flight-sql://localhost:%d?useEncryption=false" *port*)})]

      (binding [*client* client, *conn* conn]
        (f)))))

(def ^:private ^"[Lorg.apache.arrow.flight.CallOption;"
  empty-call-opts
  (make-array CallOption 0))

(defn- flight-info->rows [^FlightInfo flight-info]
  (let [ticket (.getTicket ^FlightEndpoint (first (.getEndpoints flight-info)))]
    (with-open [stream (.getStream *client* ticket empty-call-opts)]
      (let [root (.getRoot stream)
            !res (atom [])]
        (while (.next stream)
          ;; if this were a real client chances are they wouldn't just
          ;; eagerly turn the roots into Clojure maps...
          (swap! !res into (vr/rel->rows (vr/<-root root))))

        @!res))))

(t/deftest test-client
  (t/is (= -1 (.executeUpdate *client* "INSERT INTO users (xt$id, name) VALUES ('jms', 'James'), ('hak', 'Håkan')" empty-call-opts)))

  (t/is (= [{:xt$id "jms", :name "James"}
            {:xt$id "hak", :name "Håkan"}]
           (-> (.execute *client* "SELECT users.xt$id, users.name FROM users" empty-call-opts)
               (flight-info->rows)))))

(t/deftest test-jdbc-client
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO users (xt$id, name) VALUES ('jms', 'James')"]])

  ;; NOTE FSQL JDBC driver doesn't seem happy with prepared statement updates
  ;; see https://issues.apache.org/jira/browse/ARROW-18294
  #_
  (with-open [ps (jdbc/prepare *conn* ["INSERT INTO users (xt$id, name) VALUES ('jms', 'James')"])]
    ;; NOTE: next.jdbc submits everything as just `execute` rather than `executeUpdate`
    ;; FSQL depends on this difference, so we call `.executeUpdate` directly
    (.executeUpdate ps))

  ;; and ideally we'd do this with params - but this fails with 'parameter index out of range'
  ;; despite us returning some parameter metadata on the prepared statement
  #_
  (with-open [ps (jdbc/prepare *conn* ["INSERT INTO users (xt$id, name) VALUES (?, ?)"])]
    (.executeUpdate ps)
    (jdbc-prep/set-parameters ps ["hak" "Håkan"])
    (.executeUpdate ps))

  #_ ; or batches
  (jdbc/execute-batch! *conn* "INSERT INTO users (xt$id, name) VALUES (?, ?)"
                       [["jms" "James"], ["hak" "Håkan"]] {})

  (t/is (= [{:xt$id "jms", :name "James"}]
           (jdbc/execute! *conn* ["SELECT users.xt$id, users.name FROM users"])))

  (jdbc/with-transaction [tx *conn*]
    #_ ; FSQL JDBC doesn't seem happy with params in a query
    (t/is (= [] (jdbc/execute! tx ["SELECT users.xt$id FROM users WHERE users.id = ?" "foo"])))

    (with-open [ps (jdbc/prepare tx ["SELECT users.xt$id, users.name FROM users"])]
      (t/is (= [{:xt$id "jms", :name "James"}] (jdbc/execute! ps))))))

(t/deftest test-transaction
  (let [fsql-tx (.beginTransaction *client* empty-call-opts)]
    (t/is (= -1 (.executeUpdate *client* "INSERT INTO users (xt$id, name) VALUES ('jms', 'James'), ('hak', 'Håkan')" fsql-tx empty-call-opts)))
    (t/is (= []
             (-> (.execute *client* "SELECT users.xt$id, users.name FROM users" empty-call-opts)
                 (flight-info->rows))))
    (.commit *client* fsql-tx empty-call-opts)

    (t/is (= [{:xt$id "jms", :name "James"}
              {:xt$id "hak", :name "Håkan"}]
             (-> (.execute *client* "SELECT users.xt$id, users.name FROM users" empty-call-opts)
                 (flight-info->rows))))))

(t/deftest test-prepared-stmts
  (with-open [ps (.prepare *client* "INSERT INTO users (xt$id, name) VALUES (?, ?)" empty-call-opts)
              param-root (VectorSchemaRoot/create (Schema. [(types/col-type->field 'xt$id :utf8) (types/col-type->field 'name :utf8)]) tu/*allocator*)]
    (.setParameters ps param-root)

    (tu/populate-root param-root [{:xt$id "jms", :name "James"}
                                  {:xt$id "mat", :name "Matt"}])
    (.executeUpdate ps empty-call-opts)

    (tu/populate-root param-root [{:xt$id "hak", :name "Håkan"}
                                  {:xt$id "wot", :name "Dan"}])
    (.executeUpdate ps empty-call-opts))

  (with-open [ps (.prepare *client* "SELECT users.name FROM users WHERE users.xt$id >= ?" empty-call-opts)
              param-root (VectorSchemaRoot/create (Schema. [(types/col-type->field '$1 :utf8)]) tu/*allocator*)]
    (.setParameters ps param-root)

    (tu/populate-root param-root [{:$1 "l"}])
    (t/is (= [{:name "Matt"}
              {:name "Dan"}]
             (-> (.execute ps empty-call-opts)
                 (flight-info->rows))))

    (tu/populate-root param-root [{:$1 "j"}])
    (t/is (= [{:name "James"}
              {:name "Matt"}
              {:name "Dan"}]
             (-> (.execute ps empty-call-opts)
                 (flight-info->rows))))))

(t/deftest test-prepared-stmts-in-tx
  (letfn [(q []
            (-> (.execute *client* "SELECT users.name FROM users" empty-call-opts)
                (flight-info->rows)))]
    (let [fsql-tx (.beginTransaction *client* empty-call-opts)]
      (with-open [ps (.prepare *client* "INSERT INTO users (xt$id, name) VALUES (?, ?)" fsql-tx empty-call-opts)
                  param-root (VectorSchemaRoot/create (Schema. [(types/col-type->field 'xt$id :utf8) (types/col-type->field 'name :utf8)]) tu/*allocator*)]
        (.setParameters ps param-root)

        (tu/populate-root param-root [{:xt$id "jms", :name "James"}
                                      {:xt$id "mat", :name "Matt"}])
        (.executeUpdate ps empty-call-opts)

        (t/is (= [] (q)))

        (tu/populate-root param-root [{:xt$id "hak", :name "Håkan"}
                                      {:xt$id "wot", :name "Dan"}])
        (.executeUpdate ps empty-call-opts)

        (t/is (= [] (q))))

      (.commit *client* fsql-tx empty-call-opts)

      (t/is (= [{:name "James"} {:name "Matt"} {:name "Håkan"} {:name "Dan"}]
               (q))))))
