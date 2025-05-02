(ns xtdb.bench.auctionmark-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.bench.auctionmark :as am]
            [xtdb.test-util :as tu :refer [*node*]])
  (:import (java.time Clock)
           (java.util Random UUID)))

(t/use-fixtures :each tu/with-node)

(defn- ->worker [node]
  (into (b/->Worker node (Random. 112) (Clock/systemUTC) (random-uuid) (System/getProperty "user.name"))
        (am/->initial-state)))

(deftest generate-user-test
  (let [worker (->worker *node*)]
    (b/generate worker :region am/generate-region 1)
    (b/generate worker :user am/generate-user 1)

    (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :user [])
                                                    (aggregate {:count-id (row-count)}))))))
    (t/is (= 0 (am/random-id worker :users)))))

(deftest generate-categories-test
  (let [worker (->worker *node*)]
    (b/generate worker :category am/generate-category 1)

    (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :category [])
                                                    (aggregate {:count-id (row-count)}))))))
    (t/is (= 0 (am/random-id worker :categories)))))

(deftest generate-region-test
  (let [worker (->worker *node*)]
    (b/generate worker :region am/generate-region 1)

    (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :region [])
                                                    (aggregate {:count-id (row-count)}))))))
    (t/is (= 0 (am/random-id worker :regions)))))

(deftest generate-global-attribute-group-test
  (let [worker (->worker *node*)]
    (b/generate worker :category am/generate-category 1)
    (b/generate worker :gag am/generate-global-attribute-group 1)

    (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :gag [])
                                                    (aggregate {:count-id (row-count)}))))))
    (t/is (= 0 (am/random-id worker :gags)))))

(deftest generate-global-attribute-value-test
  (let [worker (->worker *node*)]
    (b/generate worker :category am/generate-category 1)
    (b/generate worker :gag am/generate-global-attribute-group 1)
    (b/generate worker :gav am/generate-global-attribute-value 1)

    (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :gav [])
                                                    (aggregate {:count-id (row-count)}))))))
    (t/is (= 0 (am/random-id worker :gavs)))))

(deftest generate-user-attributes-test
  (let [worker (->worker *node*)]
    (b/generate worker :region am/generate-region 1)
    (b/generate worker :user am/generate-user 1)
    (b/generate worker :user-attribute am/generate-user-attributes 1)
    (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :user-attribute [])
                                                    (aggregate {:count-id (row-count)}))))))
    (t/is (= 0 (am/random-id worker :user-attributes)))))

(deftest generate-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (b/generate worker :region am/generate-region 1)
      (b/generate worker :user am/generate-user 1)
      (b/generate worker :category am/generate-category 1)
      (b/generate worker :item am/generate-item 1)

      (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :item [])
                                                      (aggregate {:count-id (row-count)}))))))
      (t/is (= (UUID. (am/uuid->msb (am/->user-id 0)) 0)
               (:item-id (am/random-item worker {:status :open}))))
      (t/is (= (am/->user-id 0) (:seller-id (am/random-item worker {:status :open}))))

      (t/testing "item update"
        (let [{old-description :description} (first (xt/q *node* '(from :item [description])))
              _ (am/proc-update-item worker)
              {new-description :description} (first (xt/q *node* '(from :item [description])))]
          (t/is (not= old-description new-description)))))))

(deftest proc-get-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (b/generate worker :region am/generate-region 1)
      (b/generate worker :user am/generate-user 1)
      (b/generate worker :category am/generate-category 1)
      (b/generate worker :item am/generate-item 1)
      (b/sync-node *node*)
      (t/is (some? (first (am/proc-get-item worker)))))))

(deftest proc-new-user-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (b/generate worker :region am/generate-region 1)
      (b/generate worker :category am/generate-category 1)
      (am/proc-new-user worker)

      (t/is (= {:count-id 1} (first (xt/q *node* '(-> (from :user [])
                                                      (aggregate {:count-id (row-count)}))))))
      (t/is (= 0 (am/random-id worker :users))))))

(deftest proc-new-bid-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (t/testing "new bid"
        (b/generate worker :region am/generate-region 1)
        (b/generate worker :user am/generate-user 2)
        (b/generate worker :category am/generate-category 1)
        (b/generate worker :item am/generate-item 1)

        (am/proc-new-bid worker)

        ;; there exists a bid
        (t/is (= {:xt/id #uuid "d526fcdf-9b10-329b-0000-000000000000"
                  :item-id #uuid "d526fcdf-9b10-329b-0000-000000000000"
                  :bidder-id #uuid "d33def0e-b493-3f91-b88e-b4e784adaf05"}
                 (first (xt/q *node* '(from :item-bid [xt/id item-id bidder-id])))))
        ;; new max bid
        (t/is (= {:item-id #uuid "d526fcdf-9b10-329b-0000-000000000000"
                  :max-bid-id #uuid "d526fcdf-9b10-329b-0000-000000000000"}
                 (first (xt/q *node* '(from :item-max-bid [{:xt/id item-id} max-bid-id]))))))

      (t/testing "new bid but does not exceed max"
        (with-redefs [am/random-price (constantly Double/MIN_VALUE)]
          (b/generate worker :user am/generate-user 1)
          (am/proc-new-bid worker)

          ;; winning bid remains the same
          (t/is (= {:item-id #uuid "d526fcdf-9b10-329b-0000-000000000000"
                    :max-bid-id #uuid "d526fcdf-9b10-329b-0000-000000000000"}
                   (first (xt/q *node* '(from :item-max-bid [{:xt/id item-id} max-bid-id])))))))

      (t/testing "new exceeds max bid"
        (with-redefs [am/random-price (constantly Double/MAX_VALUE)]
          ;; (b/generate worker :user am/generate-user 1)
          (am/proc-new-bid worker)

          ;; winning bid gets superseded
          (t/is (= {:item-id #uuid "d526fcdf-9b10-329b-0000-000000000000",
                    :max-bid-id #uuid "d526fcdf-9b10-329b-0000-000000000002"}
                   (first (xt/q *node* '(from :item-max-bid [{:xt/id item-id} max-bid-id]))))))))))

(deftest proc-new-item-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (b/generate worker :region am/generate-region 1)
      (b/generate worker :user am/generate-user 1)
      (b/generate worker :category am/generate-category 10)
      (b/generate worker :gag am/generate-global-attribute-group 10)
      (b/generate worker :gav am/generate-global-attribute-value 100)
      (am/proc-new-item worker)

      ;; new item
      (let [{:keys [item-id seller-id]} (first (xt/q *node* '(from :item [{:xt/id item-id} seller-id])))]
        (t/is (= (UUID. (am/uuid->msb (am/->user-id 0)) 0)
                 item-id))
        (t/is (= (am/->user-id 0) seller-id)))

      (t/is (< (- (:balance (first (xt/q *node* '(from :user [balance]))))
                  (double -1.0))
               0.0001)))))

(deftest proc-new-comment-and-response-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)
          comment-id #uuid "d526fcdf-9b10-329b-0000-000000000000"]
      (b/generate worker :region am/generate-region 1)
      (b/generate worker :user am/generate-user 1)
      (b/generate worker :category am/generate-category 10)
      (b/generate worker :gag am/generate-global-attribute-group 10)
      (b/generate worker :gav am/generate-global-attribute-value 100)
      (b/generate worker :item am/generate-item 1)

      (am/proc-new-comment worker)

      (t/is (= [{}]
               (xt/q *node* ['#(from :item-comment [{:xt/id %} response])
                             comment-id])))

      (am/proc-new-comment-response worker)

      (t/is (true? (-> (xt/q *node* ['#(from :item-comment [{:xt/id %} response])
                                     comment-id])
                       first
                       (contains? :response)))))))

(deftest proc-new-purchase-test
  (with-redefs [am/sample-status (constantly :waiting-for-purchase)]
    (let [worker (->worker *node*)]
      (t/testing "new purchase"
        (b/generate worker :region am/generate-region 1)
        (b/generate worker :user am/generate-user 1)
        (b/generate worker :category am/generate-category 10)
        (b/generate worker :gag am/generate-global-attribute-group 10)
        (b/generate worker :gav am/generate-global-attribute-value 100)
        (b/generate worker :item am/generate-item 1)

        (t/is (= [{:xt/id #uuid "d526fcdf-9b10-329b-0000-000000000000", :status :waiting-for-purchase}]
                 (xt/q *node* '(from :item [xt/id status]))))
        (am/proc-new-purchase worker)

        (t/is (= [{:xt/id #uuid "d526fcdf-9b10-329b-0000-000000000000", :status :closed}]
                 (xt/q *node* '(from :item [xt/id status]))))

        (t/is (= [{:status :closed}]
                 (xt/q *node* '(from :item [status]))))))))

(deftest proc-new-feedback-test
  (with-redefs [am/sample-status (constantly :closed)]
    (let [worker (->worker *node*)]
      (t/testing "new feedback"
        (b/generate worker :region am/generate-region 1)
        (b/generate worker :user am/generate-user 1)
        (b/generate worker :category am/generate-category 10)
        (b/generate worker :gag am/generate-global-attribute-group 10)
        (b/generate worker :gav am/generate-global-attribute-value 100)
        (b/generate worker :item am/generate-item 1)

        (am/proc-new-feedback worker)

        (t/is (= [{:xt/id #uuid "d526fcdf-9b10-329b-0000-000000000000"}]
                 (xt/q *node* '(from :item-feedback [xt/id]))))))))

(deftest proc-check-winning-bids-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (b/generate worker :region am/generate-region 1)
      (b/generate worker :user am/generate-user 2)
      (b/generate worker :category am/generate-category 10)
      (b/generate worker :gag am/generate-global-attribute-group 10)
      (b/generate worker :gav am/generate-global-attribute-value 100)
      (b/generate worker :item am/generate-item 2)

      (am/proc-new-bid worker)
      (am/proc-check-winning-bids worker)

      ;; works as we use a pseudorandom generator
      (t/is (= [{:xt/id #uuid "d526fcdf-9b10-329b-0000-000000000000"}]
               (xt/q *node* '(from :item [{:status :closed} xt/id]))))
      (t/is (= [{:xt/id #uuid "d526fcdf-9b10-329b-0000-000000000001"}]
               (xt/q *node* '(from :item [{:status :waiting-for-purchase} xt/id])))))))

(deftest proc-get-item-comment-test
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (t/testing "non-answered-comments"
        (b/generate worker :region am/generate-region 1)
        (b/generate worker :user am/generate-user 1)
        (b/generate worker :category am/generate-category 10)
        (b/generate worker :gag am/generate-global-attribute-group 10)
        (b/generate worker :gav am/generate-global-attribute-value 100)
        (b/generate worker :item am/generate-item 1)
        (am/proc-new-comment worker)

        (t/is (= [#uuid "d526fcdf-9b10-329b-0000-000000000000"]
                 (map :xt/id (am/proc-get-comment worker))))))))

(deftest proc-get-user-info
  (with-redefs [am/sample-status (constantly :open)]
    (let [worker (->worker *node*)]
      (t/testing "non-answered-comments"
        (b/generate worker :region am/generate-region 1)
        (b/generate worker :user am/generate-user 2)
        (b/generate worker :category am/generate-category 10)
        (b/generate worker :gag am/generate-global-attribute-group 10)
        (b/generate worker :gav am/generate-global-attribute-value 100)
        (b/generate worker :item am/generate-item 1)

        (am/proc-new-bid worker)
        (am/proc-check-winning-bids worker)
        (am/index-item-status-groups! worker)
        (am/proc-new-purchase worker)
        (am/index-item-status-groups! worker)
        (am/proc-new-feedback worker)

        ;; user 0 is a seller
        (let [[user-results _item-results _feedback-results] (am/get-user-info *node* (am/->user-id 0) {:seller-items? true, :buyer-items? true, :feedback? true})]
          (t/is (= 1 (count user-results)))
          #_(t/is (= 1 (count item-results)))
          #_(t/is (= 1 (count feedback-results))))

        ;; user 1 is a buyer
        (let [[user-results _item-results _feedback] (am/get-user-info *node* (am/->user-id 1) {:seller-items? false, :buyer-items? true, :feedback? true})]
          (t/is (= 1 (count user-results)))
          #_(t/is (= 1 (count item-results))))))))
