(ns xtdb.shared-empty-buffer-tx-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu :refer [*node*]])
  (:import (org.apache.arrow.memory ArrowBuf BufferAllocator)))

(t/use-fixtures :each tu/with-node)

(defn- submit-with-shared-empty-writer-index
  "`allocator.empty` is one ArrowBuf for the whole JVM, so moving its writer index here stands in for another thread
   (e.g. an Arrow stream unloading an empty list vector) doing so while this thread serialises the transaction."
  [ops]
  (let [^ArrowBuf shared (.getEmpty ^BufferAllocator (.getAllocator *node*))]
    (.writerIndex shared 4)
    (try
      (xt/submit-tx *node* ops)
      (finally
        (.readerIndex shared 0)
        (.writerIndex shared 0)))))

(defn- tx-outcome [tx-id]
  (-> (xt/q *node* ["SELECT committed, error FROM xt.txs WHERE _id = ?" tx-id])
      first
      (select-keys [:committed :error])))

(deftest tx-submitted-while-shared-empty-buffer-is-moved-applies-as-written
  (doseq [[case-name table ops expected]
          [["SQL INSERT with args" :sql_insert_args
            [[:sql "INSERT INTO sql_insert_args RECORDS {_id: ?, v: ?}" ["b" 42] ["c" 43]]]
            [{:xt/id "a", :v 1} {:xt/id "b", :v 42} {:xt/id "c", :v 43}]]

           ["SQL INSERT without args" :sql_insert_literal
            [[:sql "INSERT INTO sql_insert_literal RECORDS {_id: 'b', v: 42}"]]
            [{:xt/id "a", :v 1} {:xt/id "b", :v 42}]]

           ["put-docs" :put_docs
            [[:put-docs :put_docs {:xt/id "b", :v 42, :s {:x "y"}, :l [1 2]}]]
            [{:xt/id "a", :v 1} {:xt/id "b", :v 42, :s {:x "y"}, :l [1 2]}]]

           ["patch-docs" :patch_docs
            [[:patch-docs :patch_docs {:xt/id "a", :v 42}]]
            [{:xt/id "a", :v 42}]]

           ["SQL UPDATE with args" :sql_update_args
            [[:sql "UPDATE sql_update_args SET v = ? WHERE _id = ?" [42 "a"]]]
            [{:xt/id "a", :v 42}]]

           ["SQL DELETE with args" :sql_delete_args
            [[:sql "DELETE FROM sql_delete_args WHERE _id = ?" ["a"]]]
            []]

           ["delete-docs" :delete_docs
            [[:delete-docs :delete_docs "a"]]
            []]]]

    (t/testing case-name
      (xt/execute-tx *node* [[:put-docs table {:xt/id "a", :v 1}]])

      (let [{:keys [tx-id]} (submit-with-shared-empty-writer-index ops)]
        (xt/execute-tx *node* [[:put-docs :await_marker {:xt/id (name table)}]])

        (t/is (= {:committed true} (tx-outcome tx-id)))
        (t/is (= expected (xt/q *node* (str "SELECT * FROM " (name table) " ORDER BY _id"))))))))
