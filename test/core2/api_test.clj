(ns core2.api-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.client :as client]
            [core2.test-util :as tu :refer [*node*]]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [core2.local-node :as node])
  (:import java.time.Duration
           java.util.concurrent.ExecutionException))

(defn- with-mock-clocks [f]
  (tu/with-opts {:core2.log/memory-log {:clock (tu/->mock-clock)}
                 :core2.tx-producer/tx-producer {:clock (tu/->mock-clock)}}
    f))

(defn- with-client [f]
  (let [port (tu/free-port)
        sys (-> {:core2/server {:node *node*, :port port}}
                ig/prep
                (doto ig/load-namespaces)
                ig/init)]
    (try
      (binding [*node* (client/start-client (str "http://localhost:" port))]
        (f))
      (finally
        (ig/halt! sys)))))

(def api-implementations
  (-> {:in-memory (t/join-fixtures [with-mock-clocks tu/with-node])
       :remote (t/join-fixtures [with-mock-clocks tu/with-node with-client])}

      #_(select-keys [:in-memory])
      #_(select-keys [:remote])))

(def ^:dynamic *node-type*)

(defn with-each-api-implementation [f]
  (doseq [[node-type run-tests] api-implementations]
    (binding [*node-type* node-type]
      (t/testing (str node-type)
        (run-tests f)))))

(t/use-fixtures :each with-each-api-implementation)

(t/deftest test-status
  (t/is (map? (c2/status *node*))))

(t/deftest test-simple-query
  (let [!tx (c2/submit-tx *node* [[:put {:_id :foo, :inst #inst "2021"}]])]
    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :tx-time (util/->instant #inst "2020-01-01")}) @!tx))

    (t/is (= [{:e :foo, :inst (util/->zdt #inst "2021")}]
             (->> (c2/plan-datalog *node*
                                   (-> '{:find [?e ?inst]
                                         :where [[?e :inst ?inst]]}
                                       (assoc :basis {:tx !tx}
                                              :basis-timeout (Duration/ofSeconds 1))))
                  (into []))))))

(t/deftest test-validation-errors
  (t/is (thrown? IllegalArgumentException
                 (try
                   @(c2/submit-tx *node* [[:pot {:_id :foo}]])
                   (catch ExecutionException e
                     (throw (.getCause e))))))

  (t/is (thrown? IllegalArgumentException
                 (try
                   @(c2/submit-tx *node* [[:put {}]])
                   (catch ExecutionException e
                     (throw (.getCause e)))))))

(t/deftest round-trips-lists
  (let [!tx (c2/submit-tx *node* [[:put {:_id :foo, :list [1 2 ["foo" "bar"]]}]])]
    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :tx-time (util/->instant #inst "2020-01-01")}) @!tx))

    (t/is (= [{:id :foo
               :list [1 2 ["foo" "bar"]]}]
             (c2/datalog-query *node*
                               (-> '{:find [?id ?list]
                                     :where [[?id :list ?list]]}
                                   (assoc :basis {:tx !tx}
                                          :basis-timeout (Duration/ofSeconds 1))))))))

(t/deftest round-trips-structs
  (let [!tx (c2/submit-tx *node* [[:put {:_id :foo, :struct {:a 1, :b {:c "bar"}}}]
                                  [:put {:_id :bar, :struct {:a true, :d 42.0}}]])]
    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :tx-time (util/->instant #inst "2020-01-01")}) @!tx))

    (t/is (= #{{:id :foo, :struct {:a 1, :b {:c "bar"}}}
               {:id :bar, :struct {:a true, :d 42.0}}}
             (set (c2/datalog-query *node*
                                    (-> '{:find [?id ?struct]
                                          :where [[?id :struct ?struct]]}
                                        (assoc :basis {:tx !tx}
                                               :basis-timeout (Duration/ofSeconds 1)))))))))

(t/deftest can-manually-specify-tx-time-47
  (let [tx1 @(c2/submit-tx *node* [[:put {:_id :foo}]]
                           {:tx-time #inst "2012"})

        _invalid-tx @(c2/submit-tx *node* [[:put {:_id :bar}]]
                                   {:tx-time #inst "2011"})

        tx3 @(c2/submit-tx *node* [[:put {:_id :baz}]])]

    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :tx-time (util/->instant #inst "2012")})
             tx1))

    (letfn [(q-at [tx]
              (->> (c2/datalog-query *node*
                                     (-> '{:find [?id]
                                           :where [[?e :_id ?id]]}
                                         (assoc :basis {:tx tx}
                                                :basis-timeout (Duration/ofSeconds 1))))
                   (into #{} (map :id))))]

      (t/is (= #{:foo} (q-at tx1)))
      (t/is (= #{:foo :baz} (q-at tx3))))))

(def ^:private devs
  [[:put {:_id :jms, :name "James"}]
   [:put {:_id :hak, :name "Håkan"}]
   [:put {:_id :mat, :name "Matt"}]
   [:put {:_id :wot, :name "Dan"}]])

(t/deftest test-sql-roundtrip
  (let [!tx (c2/submit-tx *node* devs)]

    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :tx-time (util/->instant #inst "2020-01-01")}) @!tx))

    (t/is (= [{:name "James"}]
             (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.name = 'James'"
                           {:basis {:tx !tx}})))))

(t/deftest test-sql-dynamic-params-103
  (let [!tx (c2/submit-tx *node* devs)]

    (t/is (= [{:name "James"} {:name "Matt"}]
             (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.name IN (?, ?)"
                           {:basis {:tx !tx}
                            :? ["James", "Matt"]})))))

(t/deftest start-and-query-empty-node-re-231-test
  (with-open [n (node/start-node {})]
    (t/is (= [] (c2/sql-query n "select a.a from a a" {})))))

(t/deftest test-basic-sql-dml
  (let [!tx1 (c2/submit-tx *node* [[:sql '[:insert
                                           [:table [{:_id ?id, :name ?name, :_valid-time-start ?start-date}]]]
                                    [{:?id :dave, :?name "Dave", :?start-date #inst "2018"}
                                     {:?id :claire, :?name "Claire", :?start-date #inst "2019"}
                                     {:?id :alan, :?name "Alan", :?start-date #inst "2020"}
                                     {:?id :susan, :?name "Susan", :?start-date #inst "2021"}]]])]

    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :tx-time (util/->instant #inst "2020-01-01")}) @!tx1))

    ;; TODO use `APP_TIME CONTAINS ?` here once it's available
    (t/is (= [{:name "Dave"} {:name "Claire"}]
             (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2019-06-01 00:00:00', TIMESTAMP '2019-07-01 00:00:00')"
                           {:basis {:tx !tx1}})))

    (t/is (= [{:name "Dave"} {:name "Claire"} {:name "Alan"}]
             (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2020-06-01 00:00:00', TIMESTAMP '2020-07-01 00:00:00')"
                           {:basis {:tx !tx1}})))

    (let [!tx2 (c2/submit-tx *node* [[:sql '[:delete {:_valid-time-start #inst "2020-05-01"}
                                             [:scan [_iid
                                                     {_id (= _id ?id)}
                                                     _valid-time-start
                                                     {_valid-time-end (>= _valid-time-end #inst "2020-05-01")}]]]
                                      [{:?id :dave}]]])]

      (t/is (= [{:name "Claire"} {:name "Alan"}]
               (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2020-06-01 00:00:00', TIMESTAMP '2020-07-01 00:00:00')"
                             {:basis {:tx !tx2}})))

      (t/is (= [{:name "Dave"} {:name "Claire"} {:name "Alan"}]
               (c2/sql-query *node* "SELECT users.name FROM users WHERE users.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2020-06-01 00:00:00', TIMESTAMP '2020-07-01 00:00:00')"
                             {:basis {:tx !tx1}})))

      (t/is (= [{:name "Dave"} {:name "Claire"}]
               (c2/sql-query *node* "SELECT u.name FROM users u WHERE u.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2019-06-01 00:00:00', TIMESTAMP '2019-07-01 00:00:00')"
                             {:basis {:tx !tx2}}))))))
