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

;; TODO this should likely be under src/test?

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
  (let [worker (->worker *node*)
        tx! (bcore2/generate worker am/generate-user 1 true)]
    (tu/then-await-tx tx! *node*)
    (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                 :where [[id :_table :user]
                                                         [id :u_id]]}))))
    (t/is (= "u_0" (b/sample-flat worker am/user-id)))))

(t/deftest generate-categories-test
  (let [worker (->worker *node*)
        tx! (do
              (am/load-categories-tsv worker)
              (bcore2/generate worker am/generate-category 1 true))]
    (tu/then-await-tx tx! *node*)
    (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                 :where [[id :_table :category]
                                                         [id :c_id]]}))))
    (t/is (= "c_0" (b/sample-flat worker am/category-id)))))

(t/deftest generate-region-test
  (let [worker (->worker *node*)
        tx! (bcore2/generate worker am/generate-region 1 true)]
    (tu/then-await-tx tx! *node*)
    (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                 :where [[id :_table :region]
                                                         [id :r_id]]}))))
    (t/is (= "r_0" (b/sample-flat worker am/region-id)))))

(t/deftest generate-global-attribute-group-test
  (let [worker (->worker *node*)
        tx! (do
              (am/load-categories-tsv worker)
              (bcore2/generate worker am/generate-category 1)
              (bcore2/generate worker am/generate-global-attribute-group 1 true))]
    (tu/then-await-tx tx! *node*)
    (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                 :where [[id :_table :gag]
                                                         [id :gag_name]]}))))
    (t/is (= "gag_0" (b/sample-flat worker am/gag-id)))))

(t/deftest generate-global-attribute-value-test
  (let [worker (->worker *node*)
        tx! (do
              (am/load-categories-tsv worker)
              (bcore2/generate worker am/generate-category 1)
              (bcore2/generate worker am/generate-global-attribute-group 1)
              (bcore2/generate worker am/generate-global-attribute-value 1 true))]
    (tu/then-await-tx tx! *node*)
    (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                 :where [[id :_table :gav]
                                                         [id :gav_name]]}))))
    (t/is (= "gav_0" (b/sample-flat worker am/gav-id)))))

(t/deftest generate-user-attributes-test
  (let [worker (->worker *node*)
        tx! (do
              (bcore2/generate worker am/generate-user 1)
              (bcore2/generate worker am/generate-user-attributes 1 true))]
    (tu/then-await-tx tx! *node*)
    (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                 :where [[id :_table :user-attribute]
                                                         [id :ua_u_id]]}))))
    (t/is (= "ua_0" (b/sample-flat worker am/user-attribute-id)))))

(t/deftest generate-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)
          tx! (do
                (bcore2/generate worker am/generate-user 1)
                (am/load-categories-tsv worker)
                (bcore2/generate worker am/generate-category 1)
                (bcore2/generate worker am/generate-item 1 true))]
      (tu/then-await-tx tx! *node*)
      (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                   :where [[id :_table :item]
                                                           [id :i_id]]}))))
      (t/is (= "i_0" (:i_id (am/random-item worker :status :open)))))))

(t/deftest proc-get-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)
          tx! (do
                (bcore2/generate worker am/generate-user 1)
                (am/load-categories-tsv worker)
                (bcore2/generate worker am/generate-category 1)
                (bcore2/generate worker am/generate-item 1 true))]
      (tu/then-await-tx tx! *node*)
      (t/is (= "i_0" (-> (am/proc-get-item worker) first :i_id))))))

(t/deftest proc-new-user-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)
          tx! (do
                (am/load-categories-tsv worker)
                (bcore2/generate worker am/generate-category 1)
                (bcore2/generate worker am/generate-item 1)
                (am/proc-new-user worker))]
      (tu/then-await-tx tx! *node*)
      (t/is (= {:count-id 1} (first (c2/q *node* '{:find [(count id)]
                                                   :where [[id :_table :user]
                                                           [id :u_id]]}))))
      (t/is (= "u_0" (b/sample-flat worker am/user-id))))))

(t/deftest proc-new-bid-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (t/testing "new bid"
        (let [tx1 (do
                    (bcore2/install-tx-fns worker {:apply-seller-fee am/tx-fn-apply-seller-fee, :new-bid am/tx-fn-new-bid})
                    (bcore2/generate worker am/generate-user 1)
                    (am/load-categories-tsv worker)
                    (bcore2/generate worker am/generate-category 1)
                    (bcore2/generate worker am/generate-item 1 true))
              _ (tu/then-await-tx tx1 *node*)
              tx2 (am/proc-new-bid worker)]
          (tu/then-await-tx tx2 *node*)

          ;; item has a new bid
          ;; (t/is (= nil (am/generate-new-bid-params worker)))
          (t/is (= {:i_num_bids 1}
                   (first (c2/q *node* '{:find [i_num_bids]
                                         :where [[id :_table :item]
                                                 [id :i_num_bids i_num_bids]]}))))
          ;; there exists a bid
          (t/is (= {:ib_i_id "i_0", :ib_id "ib_0"}
                   (first (c2/q *node* '{:find [ib_id ib_i_id]
                                         :where [[ib :_table :item-bid]
                                                 [ib :ib_id ib_id]
                                                 [ib :ib_i_id ib_i_id]]}))))
          ;; new max bid
          (t/is (= {:imb "ib_0-i_0", :imb_i_id "i_0"}
                   (first (c2/q *node*
                                '{:find [imb imb_i_id]
                                  :where [[imb :_table :item-max-bid]
                                          [imb :imb_i_id imb_i_id]]}))))))

      (t/testing "new bid but does not exceed max"
        (with-redefs [am/random-price (constantly Double/MIN_VALUE)]
          (let [tx (do
                     (bcore2/generate worker am/generate-user 1)
                     (am/proc-new-bid worker))]
            (tu/then-await-tx tx *node*)
            ;; new bid
            (t/is (= 2 (-> (c2/q *node*
                                 '{:find [i_num_bids]
                                   :where
                                   [[id :_table :item]
                                    [id :i_num_bids i_num_bids]]}
                                 ;; :basis {:tx tx}
                                 )
                           first :i_num_bids))))
          ;; winning bid remains the same
          (t/is (= {:imb "ib_0-i_0", :imb_i_id "i_0"}
                   (first (c2/q *node* '{:find [imb imb_i_id]
                                         :where [[imb :_table :item-max-bid]
                                                 [imb :imb_i_id imb_i_id]]} )))))))))


(t/deftest proc-new-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (t/testing "new item"
        (let [tx (do
                   (bcore2/install-tx-fns worker {:apply-seller-fee am/tx-fn-apply-seller-fee, :new-bid am/tx-fn-new-bid})
                   (bcore2/generate worker am/generate-user 1)
                   (am/load-categories-tsv worker)
                   (bcore2/generate worker am/generate-category 10)
                   (bcore2/generate worker am/generate-global-attribute-group 10)
                   (bcore2/generate worker am/generate-global-attribute-value 100)
                   (am/proc-new-item worker))]
          (tu/then-await-tx tx *node*)

          ;; new item
          (let [{:keys [i_id i_u_id]} (first (c2/q *node* '{:find [i_id i_u_id]
                                                            :where [[id :_table :item]
                                                                    [id :i_id i_id]
                                                                    [id :i_u_id i_u_id]]}))]
            (t/is (= "i_0" i_id))
            (t/is (= "u_0" i_u_id)))
          (t/is (< (- (:u_balance (first (c2/q *node* '{:find [u_balance]
                                                        :where [[u :u_id]
                                                                [u :u_balance u_balance]]})))
                      (double -1.0))
                   0.0001)))))))
