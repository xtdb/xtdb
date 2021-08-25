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
               (crux/with-tx [[:xt/put {:xt/id :foo}]]))]
    (t/is (= {:xt/id :foo} (crux/entity db :foo)))))

(t/deftest test-simple-fork
  (fix/submit+await-tx [[:xt/put {:xt/id :ivan, :name "Ivna"}]])

  (let [all-names-query '{:find [?name]
                          :where [[?e :name ?name]]}
        db (crux/db *api*)
        _ (Thread/sleep 10)    ; to ensure these two txs are at a different ms
        db2 (crux/with-tx db
              [[:xt/put {:xt/id :ivan, :name "Ivan"}]])]

    (t/is (= (crux/valid-time db) (crux/valid-time db2)))

    (t/is (= #{["Ivna"]}
             (crux/q db all-names-query)))
    (t/is (= #{["Ivan"]}
             (crux/q db2 all-names-query)))

    (t/testing "can delete an entity"
      (t/is (= #{}
               (crux/q (crux/with-tx db [[:xt/delete :ivan]])
                       all-names-query)))
      (t/is (= #{["Petr"]}
               (crux/q (crux/with-tx db [[:xt/put {:xt/id :petr, :name "Petr"}]
                                         [:xt/delete :ivan]])
                       all-names-query))))

    (t/testing "returns nil on failed match"
      (t/is (nil? (crux/with-tx db [[:xt/match :nope {:xt/id :nope}]]))))))

(t/deftest test-history
  (fix/submit+await-tx [[:xt/put {:xt/id :ivan, :name "Ivna"}]])

  (let [db (crux/db *api*)
        history (crux/entity-history (crux/with-tx db
                                       [[:xt/put {:xt/id :ivan, :name "Ivan"}]])
                                     :ivan
                                     :asc
                                     {:with-docs? true
                                      :with-corrections? true})]

    (t/is (= (crux/valid-time db)
             (:xt/valid-time (last history))))

    (t/is (= [{:xt/tx-id 0, :xt/doc {:xt/id :ivan, :name "Ivna"}}
              {:xt/tx-id 1, :xt/doc {:xt/id :ivan, :name "Ivan"}}]

             (->> history
                  (mapv #(select-keys % [:xt/tx-id :xt/doc])))))))

(t/deftest test-speculative-from-point-in-past
  (let [ivan0 {:xt/id :ivan, :name "Ivan0"}
        tt0 (:xt/tx-time (fix/submit+await-tx [[:xt/put ivan0]]))
        _ (Thread/sleep 10)      ; to ensure these two txs are at a different ms
        _tt1 (:xt/tx-time (fix/submit+await-tx [[:xt/put {:xt/id :ivan, :name "Ivan1"}]]))

        db0 (crux/db *api* tt0 tt0)]


    (t/testing "doesn't include original data after the original db cutoff"
      (let [db1 (crux/with-tx db0
                  [[:xt/put {:xt/id :petr, :name "Petr"}]])]

        (t/is (= (crux/valid-time db0) (crux/valid-time db1)))
        (t/is (= ivan0 (crux/entity db1 :ivan)))))

    (t/testing "doesn't include original data after the original db cutoff in history"
      (t/is (= [{:xt/tx-id 0, :xt/doc {:xt/id :ivan, :name "Ivan0"}}
                {:xt/tx-id 2, :xt/doc {:xt/id :ivan, :name "Ivan2"}}]
               (->> (crux/entity-history (crux/with-tx db0 [[:xt/put {:xt/id :ivan, :name "Ivan2"}]])
                                         :ivan
                                         :asc
                                         {:with-docs? true
                                          :with-corrections? true})
                    (mapv #(select-keys % [:xt/tx-id :xt/doc]))))))))

(t/deftest test-speculative-from-point-in-future
  (let [ivan0 {:xt/id :ivan, :name "Ivan0"}
        present-tx (fix/submit+await-tx [[:xt/put ivan0]])
        _ (Thread/sleep 10) ; to ensure these two txs are at a different ms

        now+10m (Date. (+ (.getTime (Date.)) (* 10 60 1000)))
        future-ivan {:xt/id :ivan, :name "Future Ivan"}
        _now+10m-tx (fix/submit+await-tx [[:xt/put future-ivan now+10m]])
        future-db (crux/db *api* now+10m)

        now+5m (Date. (+ (.getTime (Date.)) (* 5 60 1000)))
        db (crux/with-tx future-db
             [[:xt/put {:xt/id :ivan, :name "Future Ivan 2"}]
              [:xt/put {:xt/id :ivan, :name "5m Future Ivan"} now+5m]])]

    (t/is (= now+10m (crux/valid-time db)))

    (t/is (= [{:xt/tx-id 0,
               :xt/valid-time (:xt/tx-time present-tx),
               :xt/doc ivan0}
              {:xt/tx-id 2,
               :xt/valid-time now+5m,
               :xt/doc {:xt/id :ivan, :name "5m Future Ivan"}}
              {:xt/tx-id 1,
               :xt/valid-time now+10m,
               :xt/doc {:xt/id :ivan, :name "Future Ivan"}}
              {:xt/tx-id 2,
               :xt/valid-time now+10m,
               :xt/doc {:xt/id :ivan, :name "Future Ivan 2"}}]
             (->> (crux/entity-history db
                                       :ivan
                                       :asc
                                       {:with-docs? true
                                        :with-corrections? true})
                  (mapv #(select-keys % [:xt/tx-id :xt/valid-time :xt/doc])))))))

(t/deftest test-evict
  (let [ivan {:xt/id :ivan, :name "Ivan"}
        petr {:xt/id :petr, :name "Petr"}
        _tx (fix/submit+await-tx [[:xt/put ivan]
                                  [:xt/put petr]])
        db (crux/db *api*)
        db+evict (crux/with-tx db
                   [[:xt/evict :petr]])]

    (letfn [(entity-history [db eid]
              (->> (crux/entity-history db eid :asc {:with-docs? true})
                   (map #(select-keys % [:xt/tx-id :xt/doc]))))]
      (t/is (= [{:xt/tx-id 0, :xt/doc ivan}] (entity-history db :ivan)))
      (t/is (= [{:xt/tx-id 0, :xt/doc petr}] (entity-history db :petr)))
      (t/is (= [{:xt/tx-id 0, :xt/doc ivan}] (entity-history db+evict :ivan)))

      (t/is (nil? (crux/entity db+evict :petr)))
      (t/is (empty? (entity-history db+evict :petr)))

      (t/is (= #{["Ivan"]}
               (crux/q db+evict '{:find [?name]
                                  :where [[_ :name ?name]]}))))))
