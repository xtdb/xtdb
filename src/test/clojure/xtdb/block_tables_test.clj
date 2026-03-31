(ns xtdb.block-tables-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-mock-clock tu/with-allocator tu/with-node)

(deftest test-block-files-basic
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1, :v "hello"}]])
  (tu/flush-block! tu/*node*)

  (t/testing "can read block files with BETWEEN bounds"
    (let [rows (xt/q tu/*node*
                     "SELECT block_idx, tx_id, system_time, latest_processed_msg_id, table_names, boundary_replica_msg_id, file_size
                      FROM xt.block_files WHERE block_idx BETWEEN '00' AND '264'")]
      (t/is (pos? (count rows)) "should return at least one block")

      (let [row (first rows)]
        (t/is (string? (:block-idx row)))
        (t/is (number? (:latest-processed-msg-id row)))
        (t/is (string? (:table-names row)))
        (t/is (pos? (:file-size row)))))))

(deftest test-block-files-equality
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1, :v "hello"}]])
  (tu/flush-block! tu/*node*)

  (t/testing "can read block files with equality"
    (let [rows (xt/q tu/*node*
                     "SELECT block_idx FROM xt.block_files WHERE block_idx = '00'")]
      (t/is (= 1 (count rows)))
      (t/is (= "00" (:block-idx (first rows)))))))

(deftest test-block-files-requires-predicate
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1}]])
  (tu/flush-block! tu/*node*)

  (t/is (thrown-with-msg? Exception #"require block_idx bounds"
          (xt/q tu/*node* "SELECT * FROM xt.block_files"))))

(deftest test-block-files-with-params
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1, :v "hello"}]])
  (tu/flush-block! tu/*node*)

  (t/testing "parameterised BETWEEN bounds"
    (let [rows (xt/q tu/*node*
                     ["SELECT block_idx FROM xt.block_files WHERE block_idx BETWEEN ? AND ?" "00" "264"])]
      (t/is (pos? (count rows)))))

  (t/testing "parameterised equality on table-block tables"
    (let [rows (xt/q tu/*node*
                     ["SELECT row_count FROM xt.table_block_files WHERE table_name = ? AND block_idx = ?"
                      "public/foo" "00"])]
      (t/is (= 1 (count rows))))))

(deftest test-block-files-empty-range
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1}]])
  (tu/flush-block! tu/*node*)

  (let [rows (xt/q tu/*node*
                   "SELECT * FROM xt.block_files WHERE block_idx BETWEEN '41869f' AND '41869f'")]
    (t/is (empty? rows))))

(deftest test-table-block-files-basic
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1, :v "hello"}]])
  (tu/flush-block! tu/*node*)

  (t/testing "can read table-block metadata"
    (let [rows (xt/q tu/*node*
                     "SELECT table_name, block_idx, row_count, fields, hlls, partition_count
                      FROM xt.table_block_files
                      WHERE table_name = 'public/foo' AND block_idx = '00'")]
      (t/is (= 1 (count rows)))

      (let [row (first rows)]
        (t/is (= "public/foo" (:table-name row)))
        (t/is (= "00" (:block-idx row)))
        (t/is (pos? (:row-count row)))
        (t/is (string? (:fields row)))
        (t/is (string? (:hlls row)))
        (t/is (pos? (:partition-count row)))))))

(deftest test-table-block-files-requires-predicates
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1}]])
  (tu/flush-block! tu/*node*)

  (t/testing "missing both predicates"
    (t/is (thrown-with-msg? Exception #"require table_name.*AND block_idx"
            (xt/q tu/*node* "SELECT * FROM xt.table_block_files"))))

  (t/testing "missing block_idx"
    (t/is (thrown-with-msg? Exception #"require table_name.*AND block_idx"
            (xt/q tu/*node* "SELECT * FROM xt.table_block_files WHERE table_name = 'public/foo'")))))

(deftest test-table-block-file-tries-basic
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1, :v "hello"}]])
  (tu/flush-block! tu/*node*)

  (t/testing "can read trie metadata"
    (let [rows (xt/q tu/*node*
                     "SELECT table_name, block_idx, level, trie_key, data_file_size, trie_state, row_count
                      FROM xt.table_block_file_tries
                      WHERE table_name = 'public/foo' AND block_idx = '00'")]
      (t/is (pos? (count rows)) "should return at least one trie")

      (let [row (first rows)]
        (t/is (= "public/foo" (:table-name row)))
        (t/is (= "00" (:block-idx row)))
        (t/is (string? (:trie-key row)))
        (t/is (number? (:data-file-size row)))))))

(deftest test-table-block-nonexistent
  (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id 1}]])
  (tu/flush-block! tu/*node*)

  (t/testing "nonexistent table returns empty"
    (let [rows (xt/q tu/*node*
                     "SELECT * FROM xt.table_block_files
                      WHERE table_name = 'public/nonexistent' AND block_idx = '00'")]
      (t/is (empty? rows))))

  (t/testing "nonexistent block returns empty"
    (let [rows (xt/q tu/*node*
                     "SELECT * FROM xt.table_block_files
                      WHERE table_name = 'public/foo' AND block_idx = '23e7'")]
      (t/is (empty? rows)))))
