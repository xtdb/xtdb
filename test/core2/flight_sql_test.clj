(ns core2.flight-sql-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.types :as types]
            [core2.vector.indirect :as iv])
  (:import (org.apache.arrow.flight CallOption FlightClient FlightEndpoint FlightInfo Location)
           (org.apache.arrow.flight.sql FlightSqlClient)
           (org.apache.arrow.vector VectorSchemaRoot)
           org.apache.arrow.vector.types.pojo.Schema))

(def ^:dynamic *port*)
(def ^:dynamic ^FlightSqlClient *client*)

(t/use-fixtures :each
  tu/with-allocator
  (fn [f]
    (binding [*port* (tu/free-port)]
      (tu/with-opts {:core2.flight-sql/server {:port *port*}}
        f)))

  tu/with-node

  (fn [f]
    (with-open [flight-client (-> (FlightClient/builder tu/*allocator* (Location/forGrpcInsecure "127.0.0.1" *port*))
                                  (.build))
                client (FlightSqlClient. flight-client)]
      (binding [*client* client]
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
          (swap! !res into (iv/rel->rows (iv/<-root root))))

        @!res))))

(t/deftest test-client
  (t/is (= -1 (.executeUpdate *client* "INSERT INTO users (id, name) VALUES ('jms', 'James'), ('hak', 'Håkan')" empty-call-opts)))

  (t/is (= [{:id "jms", :name "James"}
            {:id "hak", :name "Håkan"}]
           (-> (.execute *client* "SELECT users.id, users.name FROM users" empty-call-opts)
               (flight-info->rows)))))

(t/deftest test-transaction
  (let [fsql-tx (.beginTransaction *client* empty-call-opts)]
    (t/is (= -1 (.executeUpdate *client* "INSERT INTO users (id, name) VALUES ('jms', 'James'), ('hak', 'Håkan')" fsql-tx empty-call-opts)))
    (t/is (= []
             (-> (.execute *client* "SELECT users.id, users.name FROM users" empty-call-opts)
                 (flight-info->rows))))
    (.commit *client* fsql-tx empty-call-opts)

    (t/is (= [{:id "jms", :name "James"}
              {:id "hak", :name "Håkan"}]
             (-> (.execute *client* "SELECT users.id, users.name FROM users" empty-call-opts)
                 (flight-info->rows))))))

(t/deftest test-prepared-stmts
  (with-open [ps (.prepare *client* "INSERT INTO users (id, name) VALUES (?, ?)" empty-call-opts)
              param-root (VectorSchemaRoot/create (Schema. [(types/col-type->field 'id :utf8) (types/col-type->field 'name :utf8)]) tu/*allocator*)]
    (.setParameters ps param-root)

    (tu/populate-root param-root [{:id "jms", :name "James"}
                                  {:id "mat", :name "Matt"}])
    (.executeUpdate ps empty-call-opts)

    (tu/populate-root param-root [{:id "hak", :name "Håkan"}
                                  {:id "wot", :name "Dan"}])
    (.executeUpdate ps empty-call-opts))

  (with-open [ps (.prepare *client* "SELECT users.name FROM users WHERE users.id >= ?" empty-call-opts)
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
      (with-open [ps (.prepare *client* "INSERT INTO users (id, name) VALUES (?, ?)" fsql-tx empty-call-opts)
                  param-root (VectorSchemaRoot/create (Schema. [(types/col-type->field 'id :utf8) (types/col-type->field 'name :utf8)]) tu/*allocator*)]
        (.setParameters ps param-root)

        (tu/populate-root param-root [{:id "jms", :name "James"}
                                      {:id "mat", :name "Matt"}])
        (.executeUpdate ps empty-call-opts)

        (t/is (= [] (q)))

        (tu/populate-root param-root [{:id "hak", :name "Håkan"}
                                      {:id "wot", :name "Dan"}])
        (.executeUpdate ps empty-call-opts)

        (t/is (= [] (q))))

      (.commit *client* fsql-tx empty-call-opts)

      (t/is (= [{:name "James"} {:name "Matt"} {:name "Håkan"} {:name "Dan"}]
               (q))))))
