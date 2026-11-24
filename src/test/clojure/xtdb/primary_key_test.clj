(ns xtdb.primary-key-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest a-table-keyed-on-a-column-upserts-on-it
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a, b, PRIMARY KEY (a))"])
  (xt/execute-tx tu/*node* ["INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')"])
  (xt/execute-tx tu/*node* ["INSERT INTO t (a, b) SELECT 3, 'w'"])
  (xt/execute-tx tu/*node* [[:put-docs :t {:a 1, :b "z"}]])

  (t/is (= [{:a 1, :b "z"} {:a 2, :b "y"} {:a 3, :b "w"}]
           (xt/q tu/*node* "SELECT a, b FROM t ORDER BY a")))

  (t/is (= [{:b "y"}] (xt/q tu/*node* ["SELECT b FROM t WHERE a = ?" 2]))))

(t/deftest a-row-is-identified-by-its-key-column
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY, b)"])
  (xt/execute-tx tu/*node* [[:put-docs :t {:a 1, :b "x"}]
                            [:put-docs :u {:xt/id 1, :b "x"}]])

  (t/is (= [{:same true}] (xt/q tu/*node* "SELECT t._iid = u._iid AS same FROM t, u"))
        "the same value keys the same IID, whichever column holds it"))

(t/deftest a-declaration-keys-the-writes-after-it-in-its-transaction
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY, b)"
                            "INSERT INTO t (a, b) SELECT 1, 'x'"
                            [:put-docs :t {:a 2, :b "y"}]])

  (t/is (= [{:a 1, :b "x"} {:a 2, :b "y"}] (xt/q tu/*node* "SELECT a, b FROM t ORDER BY a"))))

(t/deftest a-key-column-named-doc-keys-every-write
  (xt/execute-tx tu/*node* ["CREATE TABLE t (doc PRIMARY KEY, b)"])
  (xt/execute-tx tu/*node* ["INSERT INTO t (doc, b) VALUES (1, 'x')"])
  (xt/execute-tx tu/*node* [[:put-docs :t {:doc 2, :b "y"}]])
  (xt/execute-tx tu/*node* ["UPDATE t SET b = 'z' WHERE doc = 1"])

  (t/is (= [{:doc 1, :b "z"} {:doc 2, :b "y"}] (xt/q tu/*node* "SELECT doc, b FROM t ORDER BY doc"))))

(t/deftest a-key-changes-only-until-the-table-is-written-to
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY)"])
  (xt/execute-tx tu/*node* ["CREATE TABLE t (b PRIMARY KEY)"])
  (xt/execute-tx tu/*node* [[:put-docs :t {:b 1}]])

  (t/is (anomalous? [:incorrect :xtdb/primary-key-on-non-empty-table]
                    (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY)"])))

  (xt/execute-tx tu/*node* ["CREATE TABLE t (b PRIMARY KEY)"])

  (t/is (= [{:b 1}] (xt/q tu/*node* "SELECT b FROM t"))))

(t/deftest the-first-write-fixes-the-key-to-id
  (xt/execute-tx tu/*node* [[:put-docs :t {:xt/id 1, :a 1}]])

  (t/is (anomalous? [:incorrect :xtdb/primary-key-on-non-empty-table]
                    (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY)"])))

  (tu/flush-block! tu/*node*)

  (t/is (anomalous? [:incorrect :xtdb/primary-key-on-non-empty-table]
                    (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY)"]))
        "once its rows are in a block"))

(t/deftest a-table-whose-rows-are-erased-has-still-been-written-to
  (xt/execute-tx tu/*node* [[:put-docs :t {:xt/id 1}]])
  (xt/execute-tx tu/*node* [[:erase-docs :t 1]])
  (t/is (= [] (xt/q tu/*node* "SELECT * FROM t FOR ALL VALID_TIME FOR ALL SYSTEM_TIME")))

  (t/is (anomalous? [:incorrect :xtdb/primary-key-on-non-empty-table]
                    (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY)"]))))

(t/deftest a-system-column-cannot-be-the-primary-key
  (doseq [col ["_iid" "_valid_from" "_valid_to" "_system_from" "_system_to"]]
    (t/is (anomalous? [:incorrect :xtdb/invalid-primary-key-column]
                      (xt/execute-tx tu/*node* [(format "CREATE TABLE t (a, PRIMARY KEY (%s))" col)]))
          col))

  (xt/execute-tx tu/*node* ["CREATE TABLE t (_id PRIMARY KEY, a)"]))

(t/deftest a-multi-column-primary-key-is-unsupported
  (t/is (anomalous? [:unsupported :xtdb/composite-primary-key]
                    (xt/execute-tx tu/*node* ["CREATE TABLE t (a, b, PRIMARY KEY (a, b))"])))

  (t/is (anomalous? [:incorrect :xtdb/multiple-primary-keys]
                    (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY, b PRIMARY KEY)"]))))

(t/deftest a-put-missing-the-key-fails-at-submit-once-the-key-is-fixed
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY)"])

  (t/is (xt/submit-tx tu/*node* [[:put-docs :t {:xt/id 1}]])
        "while the table is empty a tx ahead in the log may declare another key, so the indexer checks it")

  (t/is (anomalous? [:incorrect :xtdb.tx/missing-id]
                    (xt/execute-tx tu/*node* [[:put-docs :t {:xt/id 1}]])))

  (xt/execute-tx tu/*node* [[:put-docs :t {:a 1}]])

  (t/is (anomalous? [:incorrect :xtdb.tx/missing-id]
                    (xt/submit-tx tu/*node* [[:put-docs :t {:xt/id 2}]]))))

(t/deftest a-declared-key-survives-a-restart
  (let [node-dir (util/->path "target/a-declared-key-survives-a-restart")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (xt/execute-tx node ["CREATE TABLE t (a PRIMARY KEY, b)"
                           "CREATE TABLE u (a PRIMARY KEY, b)"])
      (xt/execute-tx node [[:put-docs :t {:a 1, :b "x"}]]))

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (xt/execute-tx node [[:put-docs :t {:a 1, :b "y"}]
                           [:put-docs :u {:a 1, :b "y"}]])

      (t/is (= [{:b "y"}] (xt/q node "SELECT b FROM t")) "before a flush")
      (t/is (= [{:b "y"}] (xt/q node "SELECT b FROM u")) "declared on an empty table, before a flush")

      (tu/flush-block! node))

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (xt/execute-tx node [[:put-docs :t {:a 1, :b "z"}]])

      (t/is (= [{:b "z"}] (xt/q node "SELECT b FROM t")) "after a flush"))))
