(ns core2.bench2.auctionmark-test
  (:require [clojure.test :as t]
            [core2.bench2 :as b]
            [core2.bench2.auctionmark :as am]
            [core2.test-util :as tu :refer [*node*]]
            [core2.datalog :as c2]
            [core2.bench2.core2 :as bcore2])
  (:import (java.time Clock)
           (java.util Random)
           (java.util.concurrent ConcurrentHashMap)))

(t/use-fixtures :each tu/with-node)

(defn- ->worker [node]
  (let [clock (Clock/systemUTC)
        domain-state (ConcurrentHashMap.)
        custom-state (ConcurrentHashMap.)
        root-random (Random. 112)
        reports (atom [])
        worker (b/->Worker node root-random domain-state custom-state clock reports)]
    worker))

(t/deftest generate-user-test
  (let [worker (->worker *node*)]
    (bcore2/generate worker 'user am/generate-user 1)

    (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                 :keys [count-id]
                                                 :where [(match user [id])
                                                         [id :u_id]]}))))
    (t/is (= "u_0" (b/sample-flat worker am/user-id)))))

(t/deftest generate-categories-test
  (let [worker (->worker *node*)]
    (am/load-categories-tsv worker)
    (bcore2/generate worker 'category am/generate-category 1)

    (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                 :keys [count-id]
                                                 :where [(match category [id])
                                                         [id :c_id]]}))))
    (t/is (= "c_0" (b/sample-flat worker am/category-id)))))

(t/deftest generate-region-test
  (let [worker (->worker *node*)]
    (bcore2/generate worker 'region am/generate-region 1)

    (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                 :keys [count-id]
                                                 :where [(match region [id])
                                                         [id :r_id]]}))))
    (t/is (= "r_0" (b/sample-flat worker am/region-id)))))

(t/deftest generate-global-attribute-group-test
  (let [worker (->worker *node*)]
    (am/load-categories-tsv worker)
    (bcore2/generate worker 'category am/generate-category 1)
    (bcore2/generate worker 'gag am/generate-global-attribute-group 1)

    (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                 :keys [count-id]
                                                 :where [(match gag [id])
                                                         [id :gag_name]]}))))
    (t/is (= "gag_0" (b/sample-flat worker am/gag-id)))))

(t/deftest generate-global-attribute-value-test
  (let [worker (->worker *node*)]
    (am/load-categories-tsv worker)
    (bcore2/generate worker 'category am/generate-category 1)
    (bcore2/generate worker 'gag am/generate-global-attribute-group 1)
    (bcore2/generate worker 'gav am/generate-global-attribute-value 1)

    (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                 :keys [count-id]
                                                 :where [(match gav [id])
                                                         [id :gav_name]]}))))
    (t/is (= "gav_0" (b/sample-flat worker am/gav-id)))))

(t/deftest generate-user-attributes-test
  (let [worker (->worker *node*)]
    (bcore2/generate worker 'user am/generate-user 1)
    (bcore2/generate worker 'user-attribute am/generate-user-attributes 1)
    (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                 :keys [count-id]
                                                 :where [(match user-attribute [id])
                                                         [id :ua_u_id]]}))))
    (t/is (= "ua_0" (b/sample-flat worker am/user-attribute-id)))))

(t/deftest generate-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (bcore2/generate worker 'user am/generate-user 1)
      (am/load-categories-tsv worker)
      (bcore2/generate worker 'category am/generate-category 1)
      (bcore2/generate worker 'item am/generate-item 1)

      (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                   :keys [count-id]
                                                   :where [(match item [id])
                                                           [id :i_id]]}))))
      (t/is (= "i_0" (:i_id (am/random-item worker :status :open)))))))

(t/deftest proc-get-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (bcore2/generate worker 'user am/generate-user 1)
      (am/load-categories-tsv worker)
      (bcore2/generate worker 'category am/generate-category 1)
      (bcore2/generate worker 'item am/generate-item 1)

      (t/is (= "i_0" (-> (am/proc-get-item worker) first :i_id))))))

(t/deftest proc-new-user-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (am/load-categories-tsv worker)
      (bcore2/generate worker 'category am/generate-category 1)
      (bcore2/generate worker 'item am/generate-item 1)
      (am/proc-new-user worker)

      (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                   :keys [count-id]
                                                   :where [(match user [id])
                                                           [id :u_id]]}))))
      (t/is (= "u_0" (b/sample-flat worker am/user-id))))))

(t/deftest proc-new-bid-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (t/testing "new bid"
        (bcore2/install-tx-fns worker {:apply-seller-fee am/tx-fn-apply-seller-fee, :new-bid am/tx-fn-new-bid})
        (bcore2/generate worker 'user am/generate-user 1)
        (am/load-categories-tsv worker)
        (bcore2/generate worker 'category am/generate-category 1)
        (bcore2/generate worker 'item am/generate-item 1)

        (am/proc-new-bid worker)

        ;; item has a new bid
        ;; (t/is (= nil (am/generate-new-bid-params worker)))
        (t/is (= {:i_num_bids 1}
                 (first (c2/q *node* '{:find [i_num_bids]
                                       :where [(match item [id])
                                               [id :i_num_bids i_num_bids]]}))))
        ;; there exists a bid
        (t/is (= {:ib_i_id "i_0", :ib_id "ib_0"}
                 (first (c2/q *node* '{:find [ib_id ib_i_id]
                                       :where [(match item-bid {:id ib})
                                               [ib :ib_id ib_id]
                                               [ib :ib_i_id ib_i_id]]}))))
        ;; new max bid
        (t/is (= {:imb "ib_0-i_0", :imb_i_id "i_0"}
                 (first (c2/q *node*
                              '{:find [imb imb_i_id]
                                :where [(match item-max-bid {:id imb})
                                        [imb :imb_i_id imb_i_id]]})))))

      (t/testing "new bid but does not exceed max"
        (with-redefs [am/random-price (constantly Double/MIN_VALUE)]
          (bcore2/generate worker 'user am/generate-user 1)
          (am/proc-new-bid worker)

          ;; new bid
          (t/is (= 2 (-> (c2/q *node*
                               '{:find [i_num_bids]
                                 :where
                                 [(match item [id])
                                  [id :i_num_bids i_num_bids]]}
                               ;; :basis {:tx tx}
                               )
                         first :i_num_bids)))
          ;; winning bid remains the same
          (t/is (= {:imb "ib_0-i_0", :imb_i_id "i_0"}
                   (first (c2/q *node* '{:find [imb imb_i_id]
                                         :where [(match item-max-bid {:id imb})
                                                 [imb :imb_i_id imb_i_id]]} )))))))))


(t/deftest proc-new-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (t/testing "new item"
        (bcore2/install-tx-fns worker {:apply-seller-fee am/tx-fn-apply-seller-fee, :new-bid am/tx-fn-new-bid})
        (bcore2/generate worker 'user am/generate-user 1)
        (am/load-categories-tsv worker)
        (bcore2/generate worker 'category am/generate-category 10)
        (bcore2/generate worker 'gag am/generate-global-attribute-group 10)
        (bcore2/generate worker 'gav am/generate-global-attribute-value 100)
        (am/proc-new-item worker)

        ;; new item
        (let [{:keys [i_id i_u_id]} (first (c2/q *node* '{:find [i_id i_u_id]
                                                          :where [(match item [id])
                                                                  [id :i_id i_id]
                                                                  [id :i_u_id i_u_id]]}))]
          (t/is (= "i_0" i_id))
          (t/is (= "u_0" i_u_id)))
        (t/is (< (- (:u_balance (first (c2/q *node* '{:find [u_balance]
                                                      :where [(match user {:id u})
                                                              [u :u_id]
                                                              [u :u_balance u_balance]]})))
                    (double -1.0))
                 0.0001))))))
