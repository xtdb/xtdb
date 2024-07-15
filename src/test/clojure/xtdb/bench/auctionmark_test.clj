(ns xtdb.bench.auctionmark-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.bench :as b]
            [xtdb.bench.auctionmark :as am]
            [xtdb.bench.xtdb2 :as bxt2]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu :refer [*node*]])
  (:import (java.time Clock)
           (java.util Random UUID)
           (java.util.concurrent ConcurrentHashMap)))

(t/use-fixtures :each tu/with-node)

(defn- ->worker [node]
  (let [clock (Clock/systemUTC)
        domain-state (ConcurrentHashMap.)
        custom-state (ConcurrentHashMap.)
        root-random (Random. 112)
        worker (b/->Worker node root-random domain-state custom-state clock (random-uuid) (System/getProperty "user.name"))]
    worker))

(deftest generate-user-test
  (let [worker (->worker *node*)]
    (bxt2/generate worker :user am/generate-user 1)

    (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :user [])
                                                    (aggregate {:count-id (row-count)}))))))
    (t/is (= (am/user-id 0) (b/sample-flat worker am/user-id)))))

(deftest generate-categories-test
  (let [worker (->worker *node*)]
    (am/load-categories-tsv worker)
    (bxt2/generate worker :category am/generate-category 1)

    (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :category [])
                                                    (aggregate {:count-id (row-count)}))))))
    (t/is (= (am/category-id 0) (b/sample-flat worker am/category-id)))))

(deftest generate-region-test
  (let [worker (->worker *node*)]
    (bxt2/generate worker :region am/generate-region 1)

    (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :region [])
                                                    (aggregate {:count-id (row-count)}))))))
    (t/is (= (am/region-id 0) (b/sample-flat worker am/region-id)))))

(deftest generate-global-attribute-group-test
  (let [worker (->worker *node*)]
    (am/load-categories-tsv worker)
    (bxt2/generate worker :category am/generate-category 1)
    (bxt2/generate worker :gag am/generate-global-attribute-group 1)

    (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :gag [])
                                                    (aggregate {:count-id (row-count)}))))))
    (t/is (= (am/gag-id 0) (b/sample-flat worker am/gag-id)))))

(deftest generate-global-attribute-value-test
  (let [worker (->worker *node*)]
    (am/load-categories-tsv worker)
    (bxt2/generate worker :category am/generate-category 1)
    (bxt2/generate worker :gag am/generate-global-attribute-group 1)
    (bxt2/generate worker :gav am/generate-global-attribute-value 1)

    (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :gav [])
                                                    (aggregate {:count-id (row-count)}))))))
    (t/is (= (am/gav-id 0) (b/sample-flat worker am/gav-id)))))

(deftest generate-user-attributes-test
  (let [worker (->worker *node*)]
    (bxt2/generate worker :user am/generate-user 1)
    (bxt2/generate worker :user-attribute am/generate-user-attributes 1)
    (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :user-attribute [])
                                                    (aggregate {:count-id (row-count)}))))))
    (t/is (= (am/user-attribute-id 0) (b/sample-flat worker am/user-attribute-id)))))

(deftest generate-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (bxt2/generate worker :user am/generate-user 1)
      (am/load-categories-tsv worker)
      (bxt2/generate worker :category am/generate-category 1)
      (bxt2/generate worker :item am/generate-item 1)

      (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :item [])
                                                      (aggregate {:count-id (row-count)}))))))
      (t/is (= (UUID. (.getMostSignificantBits ^UUID (am/user-id 0)) (am/item-id 0))
               (:i_id (am/random-item worker :status :open))))
      (t/is (= (am/user-id 0) (:i_u_id (am/random-item worker :status :open))))

      (t/testing "item update"
        (let [{old-description :i-description} (first (xt/q *node* '(from :item [i-description])))
              _ (am/proc-update-item worker)
              {new-description :i-description} (first (xt/q *node* '(from :item [i-description])))]
          (t/is (not= old-description new-description)))))))

(deftest proc-get-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (bxt2/generate worker :user am/generate-user 1)
      (am/load-categories-tsv worker)
      (bxt2/generate worker :category am/generate-category 1)
      (bxt2/generate worker :item am/generate-item 1)
      ;; to wait for indexing
      (Thread/sleep 10)
      (t/is (not (nil? (-> (am/proc-get-item worker) first :i_id)))))))

(deftest proc-new-user-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (am/load-categories-tsv worker)
      (bxt2/generate worker :category am/generate-category 1)
      (am/proc-new-user worker)

      (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :user [])
                                                      (aggregate {:count-id (row-count)}))))))
      (t/is (= (am/user-id 0) (b/sample-flat worker am/user-id))))))

(deftest proc-new-bid-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (t/testing "new bid"
        (bxt2/generate worker :user am/generate-user 2)
        (am/load-categories-tsv worker)
        (bxt2/generate worker :category am/generate-category 1)
        (bxt2/generate worker :item am/generate-item 1)

        (am/proc-new-bid worker)

        ;; item has a new bid
        ;; (t/is (= nil (am/generate-new-bid-params worker)))
        (t/is (= {:i_num_bids 1}
                 (first (xt/q *node* '(from :item [i_num_bids])
                              {:key-fn :snake-case-keyword}))))
        ;; there exists a bid
        (t/is (= {:ib_id #uuid "d33def0e-b493-3f91-0000-000000000000",
                  :ib_i_id #uuid "d33def0e-b493-3f91-0000-000000000000",
                  :ib_buyer_id #uuid "d526fcdf-9b10-329b-9482-0f729dbb25f4"}
                 (first (xt/q *node* '(from :item-bid [ib_id ib_i_id ib_buyer_id])
                              {:key-fn :snake-case-keyword}))))
        ;; new max bid
        (t/is (= {:imb_i_id #uuid "d33def0e-b493-3f91-0000-000000000000",
                  :imb #uuid "d33def0e-b493-3f91-d33d-ef0eb4933f91"}
                 (first (xt/q *node*
                              '(from :item-max-bid [{:xt/id imb}, imb_i_id])
                              {:key-fn :snake-case-keyword})))))

      (t/testing "new bid but does not exceed max"
        (with-redefs [am/random-price (constantly Double/MIN_VALUE)]
          (bxt2/generate worker :user am/generate-user 1)
          (am/proc-new-bid worker)

          ;; new bid
          (t/is (= 2 (-> (xt/q *node* '(from :item [i_num_bids])
                               {:key-fn :snake-case-keyword})
                         first :i_num_bids)))
          ;; winning bid remains the same
          (t/is (= {:imb #uuid "d33def0e-b493-3f91-d33d-ef0eb4933f91",
                    :imb_i_id #uuid "d33def0e-b493-3f91-0000-000000000000"}
                   (first (xt/q *node* '(from :item-max-bid [{:xt/id imb} imb_i_id])
                                {:key-fn :snake-case-keyword})))))))))

(deftest proc-new-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (t/testing "new item"
        (bxt2/generate worker :user am/generate-user 1)
        (am/load-categories-tsv worker)
        (bxt2/generate worker :category am/generate-category 10)
        (bxt2/generate worker :gag am/generate-global-attribute-group 10)
        (bxt2/generate worker :gav am/generate-global-attribute-value 100)
        (am/proc-new-item worker)

        ;; new item
        (let [{:keys [i_id i_u_id]} (first (xt/q *node* '(from :item [i_id i_u_id])
                                                 {:key-fn :snake-case-keyword}))]
          (t/is (= (UUID. (.getMostSignificantBits ^UUID (am/user-id 0)) (am/item-id 0))
                   i_id))
          (t/is (= (am/user-id 0) i_u_id)))
        (t/is (< (- (:u_balance (first (xt/q *node* '(from :user [u_balance])
                                             {:key-fn :snake-case-keyword})))
                    (double -1.0))
                 0.0001))))))

(deftest proc-new-comment-and-response-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)
          ic_id #uuid "d526fcdf-9b10-329b-0000-000000000000"]
      (t/testing "new comment"
        (bxt2/generate worker :user am/generate-user 1)
        (am/load-categories-tsv worker)
        (bxt2/generate worker :category am/generate-category 10)
        (bxt2/generate worker :gag am/generate-global-attribute-group 10)
        (bxt2/generate worker :gav am/generate-global-attribute-value 100)
        (bxt2/generate worker :item am/generate-item 1)

        (am/proc-new-comment worker)

        (t/is (= [{:ic_id ic_id}]
                 (xt/q *node* '(from :item-comment [ic_id])
                       {:basis {:at-tx (am/get-tx-key worker)} :key-fn :snake-case-keyword})))

        (t/is (false? (-> (xt/q *node* '(from :item-comment [{:xt/id "ic_0"} ic_response])
                                {:key-fn :snake-case-keyword})
                          first
                          (contains? :ic_response))))

        (am/proc-new-comment-response worker)

        (t/is (true? (-> (xt/q *node* (list 'from :item-comment [{:xt/id ic_id} 'ic_response])
                               {:key-fn :snake-case-keyword})
                         first
                         (contains? :ic_response))))))))

(deftest proc-new-purchase-test
  (with-redefs [am/sample-status (constantly :waiting-for-purchase)]
    (let [worker (->worker *node*)]
      (t/testing "new purchase"
        (bxt2/generate worker :user am/generate-user 1)
        (am/load-categories-tsv worker)
        (bxt2/generate worker :category am/generate-category 10)
        (bxt2/generate worker :gag am/generate-global-attribute-group 10)
        (bxt2/generate worker :gav am/generate-global-attribute-value 100)
        (bxt2/generate worker :item am/generate-item 1)

        (t/is (= [{:i-status :waiting-for-purchase}]
                 (xt/q *node* '(from :item [i_status]))))
        (am/proc-new-purchase worker)

        (t/is (= [{:xt/id #uuid "d526fcdf-9b10-329b-0000-000000000000"}]
                 (xt/q *node* '(from :item-purchase [xt/id]))))

        (t/is (= [{:i-status :closed}]
                 (xt/q *node* '(from :item [i_status]))))))))

(deftest proc-new-feedback-test
  (with-redefs [am/sample-status (constantly :closed)]
    (let [worker (->worker *node*)]
      (t/testing "new feedback"
        (bxt2/generate worker :user am/generate-user 1)
        (am/load-categories-tsv worker)
        (bxt2/generate worker :category am/generate-category 10)
        (bxt2/generate worker :gag am/generate-global-attribute-group 10)
        (bxt2/generate worker :gav am/generate-global-attribute-value 100)
        (bxt2/generate worker :item am/generate-item 1)

        (am/proc-new-feedback worker)

        (t/is (= [{:xt/id #uuid "d526fcdf-9b10-329b-0000-000000000000"}]
                 (xt/q *node* '(from :item-feedback [xt/id]))))))))

(deftest proc-check-winning-bids-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (bxt2/generate worker :user am/generate-user 2)
      (am/load-categories-tsv worker)
      (bxt2/generate worker :category am/generate-category 10)
      (bxt2/generate worker :gag am/generate-global-attribute-group 10)
      (bxt2/generate worker :gav am/generate-global-attribute-value 100)
      (bxt2/generate worker :item am/generate-item 2)

      (am/proc-new-bid worker)
      (am/proc-check-winning-bids worker)

      ;; works as we use a pseudorandom generator
      (t/is (= [{:xt/id #uuid "d33def0e-b493-3f91-0000-000000000000"}] (xt/q *node* '(from :item [{:i_status :closed} xt/id]))))
      (t/is (= [{:xt/id #uuid "d33def0e-b493-3f91-0000-000000000001"}] (xt/q *node* '(from :item [{:i_status :waiting-for-purchase} xt/id])))))))

(deftest proc-get-item-comment-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (t/testing "non-answered-comments"
        (bxt2/generate worker :user am/generate-user 1)
        (am/load-categories-tsv worker)
        (bxt2/generate worker :category am/generate-category 10)
        (bxt2/generate worker :gag am/generate-global-attribute-group 10)
        (bxt2/generate worker :gav am/generate-global-attribute-value 100)
        (bxt2/generate worker :item am/generate-item 1)
        (am/proc-new-comment worker)

        (t/is (= [#uuid "d526fcdf-9b10-329b-0000-000000000000"]
                 (map :xt/id (am/proc-get-comment worker))))))))

(deftest proc-get-user-info
  (with-redefs [am/sample-status (constantly :open)
                b/random-bool (constantly true)]
    (let [worker (->worker *node*)]
      (t/testing "non-answered-comments"
        (bxt2/generate worker :region am/generate-region 1)
        (bxt2/generate worker :user am/generate-user 2)
        (am/load-categories-tsv worker)
        (bxt2/generate worker :category am/generate-category 10)
        (bxt2/generate worker :gag am/generate-global-attribute-group 10)
        (bxt2/generate worker :gav am/generate-global-attribute-value 100)
        (bxt2/generate worker :item am/generate-item 1)

        (am/proc-new-bid worker)
        (am/proc-check-winning-bids worker)
        (am/index-item-status-groups worker)
        (am/proc-new-purchase worker)
        (am/index-item-status-groups worker)
        (am/proc-new-feedback worker)

        ;; user 0 is a seller
        (let [[user-results item-results feedback-results] (am/get-user-info *node* (am/user-id 0) true true true
                                                                             (am/get-tx-key worker))]

          (t/is (= 1 (count user-results)))
          #_(t/is (= 1 (count item-results)))
          #_(t/is (= 1 (count feedback-results))))

        ;; user 1 is a buyer
        (let [[user-results item-results _] (am/get-user-info *node* (am/user-id 1) false true true
                                                              (am/get-tx-key worker))]

          (t/is (= 1 (count user-results)))
          #_(t/is (= 1 (count item-results))))))))
