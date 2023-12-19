(ns xtdb.docs.reference.special-tables
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(deftest test-examples
  (let [*node* tu/*node*]
    ;; TODO: Remove when this is fixed:
    ;;       https://github.com/xtdb/xtdb/issues/3061
    (xt/submit-tx *node*
      [(xt/put :users {:xt/id :not-john})])

    ;; tag::try-update[]
    (xt/submit-tx *node*
      [(xt/assert-exists '(from :users [{:xt/id :john}]))
       (xt/update-table :users '{:bind [{:xt/id :john :age age}]
                                 :set {:age (inc age)}})])
    ;; end::try-update[]

    (t/is (= ;; tag::query-result[]
             {:xt/id 1,
              :xt/committed? false,
              :xt/error {:xtdb.error/error-type :runtime-error,
                         :xtdb.error/error-key :xtdb/assert-failed,
                         :xtdb.error/message "Precondition failed: assert-exists",
                         :row-count 0}}
             ;; end::query-result[]

             ;; tag::query-txs[]
             (-> (xt/q *node*
                   '(-> (from :xt/txs [xt/id xt/committed? xt/error])
                        (order-by xt/id)))
                 last
                 (update :xt/error ex-data))
             ;; end::query-txs[]
             ,))))
