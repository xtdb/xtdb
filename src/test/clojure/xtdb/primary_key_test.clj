(ns xtdb.primary-key-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as jdbc-rs]
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

(t/deftest id-is-an-ordinary-column-on-a-table-keyed-elsewhere
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY)"])
  (xt/execute-tx tu/*node* [[:put-docs :t {:a 1, :xt/id #xt/date "2020-01-01"}]
                            [:put-docs :t {:a 2, :xt/id {:x 1}}]
                            [:put-docs :t {:a 3}]])
  (xt/execute-tx tu/*node* ["INSERT INTO t (a, _id) VALUES (4, 4.5)"])
  (xt/execute-tx tu/*node* ["UPDATE t SET _id = 3.5 WHERE a = 3"])

  (t/is (= [{:a 1, :xt/id #xt/date "2020-01-01"} {:a 2, :xt/id {:x 1}} {:a 3, :xt/id 3.5} {:a 4, :xt/id 4.5}]
           (xt/q tu/*node* "SELECT a, _id FROM t ORDER BY a"))))

(t/deftest a-self-join-on-id-in-a-table-keyed-elsewhere
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY)"])
  (xt/execute-tx tu/*node* [[:put-docs :t {:a 1, :xt/id #xt/date "2020-01-01"} {:a 2, :xt/id {:x 1}} {:a 3, :xt/id 3}]])

  (t/is (= [{:a1 1, :a2 1} {:a1 2, :a2 2} {:a1 3, :a2 3}]
           (xt/q tu/*node* "SELECT t1.a AS a1, t2.a AS a2 FROM t t1 JOIN t t2 ON t1._id = t2._id ORDER BY t1.a"))))

(t/deftest an-id-of-a-type-that-cant-key-a-row-is-rejected
  (t/is (anomalous? [:incorrect :xtdb/invalid-id]
                    (xt/execute-tx tu/*node* [[:put-docs :t {:xt/id #xt/date "2020-01-01"}]]))
        "by the indexer, while the table's key may yet change")

  (xt/execute-tx tu/*node* [[:put-docs :t {:xt/id 1}]])

  (t/is (anomalous? [:incorrect :xtdb/invalid-id]
                    (xt/submit-tx tu/*node* [[:put-docs :t {:xt/id {:x 1}}]]))
        "at submit, once the key is fixed"))

(t/deftest select-star-lists-the-key-column-first
  (xt/execute-tx tu/*node* ["CREATE TABLE t (b PRIMARY KEY, a)"])
  (xt/execute-tx tu/*node* [[:put-docs :t {:b 1, :a 2, :xt/id 3}]
                            [:put-docs :u {:xt/id 1, :a 2}]])

  (with-open [conn (jdbc/get-connection tu/*node*)]
    (t/is (= [:b :_id :a]
             (first (jdbc/execute! conn ["SELECT * FROM t"] {:builder-fn jdbc-rs/as-unqualified-arrays}))))

    (t/is (= [:_id :a]
             (first (jdbc/execute! conn ["SELECT * FROM u"] {:builder-fn jdbc-rs/as-unqualified-arrays}))))))

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
