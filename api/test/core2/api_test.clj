(ns core2.api-test
  (:require [clojure.test :as t]
            [core2.api :as c2]
            [core2.client :as client]
            [core2.log :as log]
            [core2.test-util :as tu :refer [*node*]]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import java.time.Duration
           java.util.concurrent.ExecutionException
           core2.api.TransactionInstant))

(defmethod ig/init-key ::clock [_ _]
  (tu/->mock-clock))

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
  (-> {:in-memory tu/with-node
       :remote (t/join-fixtures [tu/with-node with-client])}

      #_(select-keys [:in-memory])
      #_(select-keys [:remote])))

(def ^:dynamic *node-type*)

(defn with-each-api-implementation [f]
  (doseq [[node-type run-tests] api-implementations]
    (binding [*node-type* node-type]
      (t/testing (str node-type)
        (run-tests f)))))

(t/use-fixtures :each
  (tu/with-opts {::clock {}
                 ::log/memory-log {:clock (ig/ref ::clock)}})
  with-each-api-implementation)

(t/deftest test-status
  (t/is (map? (c2/status *node*))))

(t/deftest test-simple-query
  (let [!tx (c2/submit-tx *node* [[:put {:_id "foo"}]])]
    (t/is (= #c2/tx-key {:tx-id 0, :tx-time #c2/instant "2020-01-01"} @!tx))

    (t/is (= [{:id "foo"}]
             (->> (c2/plan-query *node*
                                 (-> '{:find [?id]
                                       :where [[?e :_id ?id]]}
                                     (assoc :basis {:tx !tx}
                                            :basis-timeout (Duration/ofSeconds 1))))
                  (into []))))))

(t/deftest test-validation-errors
  (t/is (thrown? IllegalArgumentException
                 (try
                   @(c2/submit-tx *node* [[:pot {:_id "foo"}]])
                   (catch ExecutionException e
                     (throw (.getCause e))))))

  (t/is (thrown? IllegalArgumentException
                 (try
                   @(c2/submit-tx *node* [[:put {}]])
                   (catch ExecutionException e
                     (throw (.getCause e)))))))

(t/deftest round-trips-lists
  (let [!tx (c2/submit-tx *node* [[:put {:_id "foo", :list [1 2 ["foo" "bar"]]}]])]
    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :tx-time #c2/instant "2020-01-01"}) @!tx))

    (t/is (= [{:id "foo"
               :list [1 2 ["foo" "bar"]]}]
             (c2/query *node*
                       (-> '{:find [?id ?list]
                             :where [[?id :list ?list]]}
                           (assoc :basis {:tx !tx}
                                  :basis-timeout (Duration/ofSeconds 1))))))))

(t/deftest round-trips-structs
  (let [!tx (c2/submit-tx *node* [[:put {:_id "foo", :struct {:a 1, :b {:c "bar"}}}]
                                  [:put {:_id "bar", :struct {:a true, :d 42.0}}]])]
    (t/is (= (c2/map->TransactionInstant {:tx-id 0, :tx-time #c2/instant "2020-01-01"}) @!tx))

    (t/is (= #{{:id "foo", :struct {:a 1, :b {:c "bar"}}}
               {:id "bar", :struct {:a true, :d 42.0}}}
             (set (c2/query *node*
                            (-> '{:find [?id ?struct]
                                  :where [[?id :struct ?struct]]}
                                (assoc :basis {:tx !tx}
                                       :basis-timeout (Duration/ofSeconds 1)))))))))
