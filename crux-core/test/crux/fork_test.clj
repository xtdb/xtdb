(ns crux.fork-test
  (:require [clojure.test :as t]
            [crux.api :as crux]
            [crux.db :as db]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.tx :as tx])
  (:import java.util.Date))

(t/use-fixtures :each fix/with-node)

(t/deftest test-empty-fork
  (let [db (-> (crux/db *api*)
               (crux/with-tx [[:crux.tx/put {:xt/id :foo}]]))]
    (t/is (= {:xt/id :foo} (crux/entity db :foo)))))

(t/deftest test-simple-fork
  (fix/submit+await-tx [[:crux.tx/put {:xt/id :ivan, :name "Ivna"}]])

  (let [all-names-query '{:find [?name]
                          :where [[?e :name ?name]]}
        db (crux/db *api*)
        _ (Thread/sleep 10)    ; to ensure these two txs are at a different ms
        db2 (crux/with-tx db
              [[:crux.tx/put {:xt/id :ivan, :name "Ivan"}]])]

    (t/is (= (crux/valid-time db) (crux/valid-time db2)))

    (t/is (= #{["Ivna"]}
             (crux/q db all-names-query)))
    (t/is (= #{["Ivan"]}
             (crux/q db2 all-names-query)))

    (t/testing "can delete an entity"
      (t/is (= #{}
               (crux/q (crux/with-tx db [[:crux.tx/delete :ivan]])
                       all-names-query)))
      (t/is (= #{["Petr"]}
               (crux/q (crux/with-tx db [[:crux.tx/put {:xt/id :petr, :name "Petr"}]
                                         [:crux.tx/delete :ivan]])
                       all-names-query))))

    (t/testing "returns nil on failed match"
      (t/is (nil? (crux/with-tx db [[:crux.tx/match :nope {:xt/id :nope}]]))))))

(t/deftest test-history
  (fix/submit+await-tx [[:crux.tx/put {:xt/id :ivan, :name "Ivna"}]])

  (let [db (crux/db *api*)
        history (crux/entity-history (crux/with-tx db
                                       [[:crux.tx/put {:xt/id :ivan, :name "Ivan"}]])
                                     :ivan
                                     :asc
                                     {:with-docs? true
                                      :with-corrections? true})]

    (t/is (= (crux/valid-time db)
             (:crux.db/valid-time (last history))))

    (t/is (= [{:crux.tx/tx-id 0, :crux.db/doc {:xt/id :ivan, :name "Ivna"}}
              {:crux.tx/tx-id 1, :crux.db/doc {:xt/id :ivan, :name "Ivan"}}]

             (->> history
                  (mapv #(select-keys % [::tx/tx-id :crux.db/doc])))))))

(t/deftest test-speculative-from-point-in-past
  (let [ivan0 {:xt/id :ivan, :name "Ivan0"}
        tt0 (::tx/tx-time (fix/submit+await-tx [[:crux.tx/put ivan0]]))
        _ (Thread/sleep 10)      ; to ensure these two txs are at a different ms
        _tt1 (::tx/tx-time (fix/submit+await-tx [[:crux.tx/put {:xt/id :ivan, :name "Ivan1"}]]))

        db0 (crux/db *api* tt0 tt0)]


    (t/testing "doesn't include original data after the original db cutoff"
      (let [db1 (crux/with-tx db0
                  [[:crux.tx/put {:xt/id :petr, :name "Petr"}]])]

        (t/is (= (crux/valid-time db0) (crux/valid-time db1)))
        (t/is (= ivan0 (crux/entity db1 :ivan)))))

    (t/testing "doesn't include original data after the original db cutoff in history"
      (t/is (= [{:crux.tx/tx-id 0, :crux.db/doc {:xt/id :ivan, :name "Ivan0"}}
                {:crux.tx/tx-id 2, :crux.db/doc {:xt/id :ivan, :name "Ivan2"}}]
               (->> (crux/entity-history (crux/with-tx db0 [[:crux.tx/put {:xt/id :ivan, :name "Ivan2"}]])
                                         :ivan
                                         :asc
                                         {:with-docs? true
                                          :with-corrections? true})
                    (mapv #(select-keys % [::tx/tx-id :crux.db/doc]))))))))

(t/deftest test-speculative-from-point-in-future
  (let [ivan0 {:xt/id :ivan, :name "Ivan0"}
        present-tx (fix/submit+await-tx [[:crux.tx/put ivan0]])
        _ (Thread/sleep 10) ; to ensure these two txs are at a different ms

        now+10m (Date. (+ (.getTime (Date.)) (* 10 60 1000)))
        future-ivan {:xt/id :ivan, :name "Future Ivan"}
        _now+10m-tx (fix/submit+await-tx [[:crux.tx/put future-ivan now+10m]])
        future-db (crux/db *api* now+10m)

        now+5m (Date. (+ (.getTime (Date.)) (* 5 60 1000)))
        db (crux/with-tx future-db
             [[:crux.tx/put {:xt/id :ivan, :name "Future Ivan 2"}]
              [:crux.tx/put {:xt/id :ivan, :name "5m Future Ivan"} now+5m]])]

    (t/is (= now+10m (crux/valid-time db)))

    (t/is (= [{:crux.tx/tx-id 0,
               :crux.db/valid-time (::tx/tx-time present-tx),
               :crux.db/doc ivan0}
              {:crux.tx/tx-id 2,
               :crux.db/valid-time now+5m,
               :crux.db/doc {:xt/id :ivan, :name "5m Future Ivan"}}
              {:crux.tx/tx-id 1,
               :crux.db/valid-time now+10m,
               :crux.db/doc {:xt/id :ivan, :name "Future Ivan"}}
              {:crux.tx/tx-id 2,
               :crux.db/valid-time now+10m,
               :crux.db/doc {:xt/id :ivan, :name "Future Ivan 2"}}]
             (->> (crux/entity-history db
                                       :ivan
                                       :asc
                                       {:with-docs? true
                                        :with-corrections? true})
                  (mapv #(select-keys % [::tx/tx-id :crux.db/valid-time :crux.db/doc])))))))

(t/deftest test-evict
  (let [ivan {:xt/id :ivan, :name "Ivan"}
        petr {:xt/id :petr, :name "Petr"}
        _tx (fix/submit+await-tx [[:crux.tx/put ivan]
                                  [:crux.tx/put petr]])
        db (crux/db *api*)
        db+evict (crux/with-tx db
                   [[:crux.tx/evict :petr]])]

    (letfn [(entity-history [db eid]
              (->> (crux/entity-history db eid :asc {:with-docs? true})
                   (map #(select-keys % [::tx/tx-id ::db/doc]))))]
      (t/is (= [{::tx/tx-id 0, ::db/doc ivan}] (entity-history db :ivan)))
      (t/is (= [{::tx/tx-id 0, ::db/doc petr}] (entity-history db :petr)))
      (t/is (= [{::tx/tx-id 0, ::db/doc ivan}] (entity-history db+evict :ivan)))

      (t/is (nil? (crux/entity db+evict :petr)))
      (t/is (empty? (entity-history db+evict :petr)))

      (t/is (= #{["Ivan"]}
               (crux/q db+evict '{:find [?name]
                                  :where [[_ :name ?name]]}))))))
