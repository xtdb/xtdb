(ns xtdb.primary-key-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as jdbc-rs]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (org.apache.arrow.memory RootAllocator)
           (xtdb.arrow Vector)
           (xtdb.tx TxOp$DeleteDocs TxOp$EraseDocs TxOpts)
           xtdb.util.Iid))

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
  (xt/execute-tx tu/*node* [[:patch-docs :t {:doc 2, :c "v"}]])
  (xt/execute-tx tu/*node* ["PATCH INTO t RECORDS {doc: 1, c: 'w'}"])

  (t/is (= [{:doc 1, :b "z", :c "w"} {:doc 2, :b "y", :c "v"}] (xt/q tu/*node* "SELECT doc, b, c FROM t ORDER BY doc"))))

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
          col)

    (t/is (anomalous? [:incorrect :xtdb/invalid-primary-key-column]
                      (xt/execute-tx tu/*node* [(format "CREATE TABLE t (a, PRIMARY KEY (a, %s))" col)]))
          (str col ", as a composite key's second column")))

  (xt/execute-tx tu/*node* ["CREATE TABLE t (_id PRIMARY KEY, a)"]))

(t/deftest a-primary-key-names-each-column-once
  (t/is (anomalous? [:incorrect :xtdb/invalid-primary-key-column]
                    (xt/execute-tx tu/*node* ["CREATE TABLE t (a, b, PRIMARY KEY (a, b, a))"])))

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

(t/deftest delete-and-erase-docs-address-the-row-its-key-identifies
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY, b)"
                            [:put-docs :t {:a 1, :b "x", :xt/id 2} {:a 2, :b "y"} {:a 3, :b "z"}]])

  (xt/submit-tx tu/*node* [[:delete-docs :t 2]])
  (xt/execute-tx tu/*node* [[:erase-docs :t 3]])

  (t/is (= [{:a 1, :b "x"}] (xt/q tu/*node* "SELECT a, b FROM t FOR ALL VALID_TIME WHERE a <> 2")))
  (t/is (= [{:a 1}] (xt/q tu/*node* "SELECT a FROM t"))))

(t/deftest a-delete-by-a-value-that-cant-key-a-row-is-rejected
  (t/is (anomalous? [:incorrect :xtdb.tx/invalid-eid]
                    (xt/execute-tx tu/*node* [[:delete-docs :t 1.5]])))

  (t/is (anomalous? [:incorrect :xtdb.tx/invalid-eid]
                    (xt/execute-tx tu/*node* [[:erase-docs :t 7N]]))))

(t/deftest patch-docs-patches-the-row-its-key-identifies
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY, b, c)"])
  (xt/execute-tx tu/*node* [[:put-docs :t {:a 1, :b "x", :xt/id "one"} {:a 2, :b "y"}]])
  (xt/execute-tx tu/*node* [[:patch-docs :t {:a 1, :c "z", :xt/id "uno"}]
                            [:patch-docs :t {:a 2, :c "v"} {:a 3, :c "w", :xt/id #xt/date "2020-01-01"}]])

  (t/is (= [{:a 1, :b "x", :c "z", :xt/id "uno"} {:a 2, :b "y", :c "v"} {:a 3, :c "w", :xt/id #xt/date "2020-01-01"}]
           (xt/q tu/*node* "SELECT a, b, c, _id FROM t ORDER BY a"))))

(t/deftest sql-patch-patches-the-row-its-key-identifies
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY, b, c)"])
  (xt/execute-tx tu/*node* ["INSERT INTO t (a, b) VALUES (1, 'x'), (2, 'y')"])
  (xt/execute-tx tu/*node* ["PATCH INTO t RECORDS {a: 1, c: 'z'}, {a: 3, c: 'w'}"])

  (t/is (= [{:a 1, :b "x", :c "z"} {:a 2, :b "y"} {:a 3, :c "w"}]
           (xt/q tu/*node* "SELECT a, b, c FROM t ORDER BY a"))))

(t/deftest a-planned-sql-patch-keys-a-narrow-integer-as-patch-docs-does
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY, b, c)"])
  (xt/execute-tx tu/*node* [[:put-docs :t {:a (int 1), :b "x"} {:a (short 2), :b "y"} {:a (byte 3), :b "z"}]])
  (xt/execute-tx tu/*node* ["PATCH INTO t RECORDS {a: CAST(1 AS INT), c: 'p'}, {a: CAST(2 AS SMALLINT), c: 'q'}"])
  (xt/execute-tx tu/*node* [[:patch-docs :t {:a (byte 3), :c "r"}]])

  (t/is (= [{:a 1, :b "x", :c "p"} {:a 2, :b "y", :c "q"} {:a 3, :b "z", :c "r"}]
           (xt/q tu/*node* "SELECT a, b, c FROM t ORDER BY a"))))

(t/deftest a-patch-keys-on-a-declaration-earlier-in-its-transaction
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY, b)"
                            [:put-docs :t {:a 1, :b "x"}]
                            [:patch-docs :t {:a 1, :c "z"}]])

  (t/is (= [{:a 1, :b "x", :c "z"}] (xt/q tu/*node* "SELECT a, b, c FROM t"))))

(t/deftest a-patch-missing-the-key-is-rejected
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a PRIMARY KEY)"])

  (t/is (anomalous? [:incorrect :xtdb.tx/missing-id]
                    (xt/execute-tx tu/*node* [[:patch-docs :t {:xt/id 1}]]))
        "by the indexer, while the table's key may yet change")

  (xt/execute-tx tu/*node* [[:put-docs :t {:a 1}]])

  (t/is (anomalous? [:incorrect :xtdb.tx/missing-id]
                    (xt/submit-tx tu/*node* [[:patch-docs :t {:xt/id 1}]]))
        "at submit, once the key is fixed"))

(t/deftest select-star-lists-the-key-column-first
  (xt/execute-tx tu/*node* ["CREATE TABLE t (b PRIMARY KEY, a)"])
  (xt/execute-tx tu/*node* [[:put-docs :t {:b 1, :a 2, :xt/id 3}]
                            [:put-docs :u {:xt/id 1, :a 2}]])

  (with-open [conn (jdbc/get-connection tu/*node*)]
    (t/is (= [:b :_id :a]
             (first (jdbc/execute! conn ["SELECT * FROM t"] {:builder-fn jdbc-rs/as-unqualified-arrays}))))

    (t/is (= [:_id :a]
             (first (jdbc/execute! conn ["SELECT * FROM u"] {:builder-fn jdbc-rs/as-unqualified-arrays}))))

    (jdbc/execute! conn ["CREATE TABLE v (a, b, c, PRIMARY KEY (c, a))"])
    (jdbc/execute! conn ["INSERT INTO v (a, b, c) VALUES (1, 2, 3)"])

    (t/is (= [:c :a :b]
             (first (jdbc/execute! conn ["SELECT * FROM v"] {:builder-fn jdbc-rs/as-unqualified-arrays})))
          "a composite key's columns, in key order")))

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

(t/deftest a-table-keyed-on-several-columns-upserts-on-all-of-them
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a, b, v, PRIMARY KEY (a, b))"])
  (xt/execute-tx tu/*node* ["INSERT INTO t (a, b, v) VALUES (1, 'x', 'p'), (1, 'y', 'q'), (2, 'x', 'r')"])
  (xt/execute-tx tu/*node* ["INSERT INTO t (a, b, v) SELECT 2, 'y', 's'"])
  (xt/execute-tx tu/*node* [[:put-docs :t {:a 1, :b "x", :v "t"}]])
  (xt/execute-tx tu/*node* ["UPDATE t SET v = 'u' WHERE a = 2 AND b = 'x'"])

  (t/is (= [{:a 1, :b "x", :v "t"} {:a 1, :b "y", :v "q"} {:a 2, :b "x", :v "u"} {:a 2, :b "y", :v "s"}]
           (xt/q tu/*node* "SELECT a, b, v FROM t ORDER BY a, b")))

  (t/is (= [{:v "q"}] (xt/q tu/*node* ["SELECT v FROM t WHERE a = ? AND b = ?" 1 "y"]))))

(t/deftest a-composite-key-keys-its-row-by-the-key-iid-of-its-values
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a, b, PRIMARY KEY (a, b))"
                            "CREATE TABLE u (b, a, PRIMARY KEY (b, a))"])
  (xt/execute-tx tu/*node* [[:put-docs :t {:a 1, :b "x"}]
                            [:put-docs :u {:a 1, :b "x"}]])

  (t/is (= [{:same true}] (xt/q tu/*node* ["SELECT _iid = ? AS same FROM t" (Iid/keyIid [1 "x"])])))

  (t/is (= [{:same false}] (xt/q tu/*node* "SELECT t._iid = u._iid AS same FROM t, u"))
        "the same values in another key order key another IID"))

(t/deftest patches-address-the-row-every-key-column-identifies
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a, b, v, w, PRIMARY KEY (a, b))"])
  (xt/execute-tx tu/*node* [[:put-docs :t {:a 1, :b "x", :v "p"} {:a 1, :b "y", :v "q"}]])
  (xt/execute-tx tu/*node* [[:patch-docs :t {:a 1, :b "x", :w 1}]])
  (xt/execute-tx tu/*node* ["PATCH INTO t RECORDS {a: 1, b: 'y', w: 2}, {a: 2, b: 'x', w: 3}"])

  (t/is (= [{:a 1, :b "x", :v "p", :w 1} {:a 1, :b "y", :v "q", :w 2} {:a 2, :b "x", :w 3}]
           (xt/q tu/*node* "SELECT a, b, v, w FROM t ORDER BY a, b"))))

(t/deftest a-write-missing-a-key-column-is-rejected
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a, b, v, PRIMARY KEY (a, b))"
                            [:put-docs :t {:a 1, :b "x"}]])

  (t/is (anomalous? [:incorrect :xtdb.tx/missing-id]
                    (xt/execute-tx tu/*node* [[:put-docs :t {:a 2, :v "p"}]])))

  (t/is (anomalous? [:incorrect :xtdb.tx/missing-id]
                    (xt/execute-tx tu/*node* [[:patch-docs :t {:a 1, :v "p"}]])))

  (t/is (anomalous? [:incorrect :xtdb.expression/unknown-symbol]
                    (xt/execute-tx tu/*node* ["PATCH INTO t RECORDS {a: 1 + 0, v: 'p'}"]))
        "a planned PATCH")

  (t/is (anomalous? [:incorrect :xtdb.tx/missing-id]
                    (xt/execute-tx tu/*node* ["INSERT INTO t (a, v) VALUES (2, 'p')"]))
        "a static INSERT is a put")

  (t/is (anomalous? [:incorrect nil #"INSERT does not contain mandatory b column"]
                    (xt/execute-tx tu/*node* ["INSERT INTO t (a, v) SELECT 2, 'p'"])))

  (t/is (anomalous? [:incorrect nil #"Cannot UPDATE b column"]
                    (xt/execute-tx tu/*node* ["UPDATE t SET b = 'y' WHERE a = 1"])))

  (t/is (= [{:a 1, :b "x"}] (xt/q tu/*node* "SELECT a, b, v FROM t"))))

(t/deftest a-composite-key-survives-a-restart
  (let [node-dir (util/->path "target/a-composite-key-survives-a-restart")]
    (util/delete-dir node-dir)

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (xt/execute-tx node ["CREATE TABLE t (a, b, v, PRIMARY KEY (a, b))"])
      (xt/execute-tx node [[:put-docs :t {:a 1, :b "x", :v "p"} {:a 1, :b "y", :v "q"}]]))

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (xt/execute-tx node [[:put-docs :t {:a 1, :b "x", :v "r"}]])

      (t/is (= [{:b "x", :v "r"} {:b "y", :v "q"}] (xt/q node "SELECT b, v FROM t ORDER BY b")) "before a flush")

      (tu/flush-block! node))

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (xt/execute-tx node ["PATCH INTO t RECORDS {a: 1, b: 'y', v: 's'}"])

      (t/is (= [{:b "x", :v "r"} {:b "y", :v "s"}] (xt/q node "SELECT b, v FROM t ORDER BY b")) "after a flush"))))

(t/deftest delete-and-erase-docs-take-a-composite-keys-values-as-one-vector-per-row
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a, b, PRIMARY KEY (a, b))"
                            [:put-docs :t {:a 1, :b "x"} {:a 1, :b "y"} {:a 2, :b "x"} {:a 2, :b "y"}]])

  (xt/submit-tx tu/*node* [[:delete-docs :t [1 "x"] [2 "y"]]])
  (xt/execute-tx tu/*node* [[:erase-docs :t [2 "x"]]])

  (t/is (= [{:a 1, :b "x"} {:a 1, :b "y"} {:a 2, :b "y"}]
           (xt/q tu/*node* "SELECT DISTINCT a, b FROM t FOR ALL VALID_TIME ORDER BY a, b")))
  (t/is (= [{:a 1, :b "y"}] (xt/q tu/*node* "SELECT a, b FROM t"))))

(t/deftest a-row-key-of-the-wrong-arity-is-rejected-at-submit
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a, b, PRIMARY KEY (a, b))"
                            [:put-docs :t {:a 1, :b 2}]
                            [:put-docs :u {:xt/id 1}]])

  (t/is (anomalous? [:incorrect :xtdb.tx/key-arity] (xt/submit-tx tu/*node* [[:delete-docs :t 1]]))
        "a single value against a composite key")
  (t/is (anomalous? [:incorrect :xtdb.tx/key-arity] (xt/execute-tx tu/*node* [[:erase-docs :t [1 2] [1 2 3]]]))
        "a vector with a value too many")
  (t/is (anomalous? [:incorrect :xtdb.tx/key-arity] (xt/execute-tx tu/*node* [[:delete-docs :u [1 2]]]))
        "a vector against a single-column key")

  (t/is (anomalous? [:incorrect :xtdb.tx/invalid-eid] (xt/execute-tx tu/*node* [[:delete-docs :t []]])))
  (t/is (anomalous? [:incorrect :xtdb.tx/invalid-eid] (xt/execute-tx tu/*node* [[:delete-docs :t [1 [2]]]])))

  (t/is (= [{:a 1, :b 2}] (xt/q tu/*node* "SELECT a, b FROM t")))
  (t/is (= [{:xt/id 1}] (xt/q tu/*node* "SELECT _id FROM u"))))

(t/deftest tx-op-delete-docs-and-erase-docs-take-row-keys
  (xt/execute-tx tu/*node* ["CREATE TABLE t (a, b, PRIMARY KEY (a, b))"
                            [:put-docs :t {:a 1, :b "x"} {:a 2, :b "y"}]])

  (with-open [al (RootAllocator.)]
    (with-open [del (TxOp$DeleteDocs. "public" "t" nil nil (Vector/fromList al "ids" [[1 "x"]]))
                erase (TxOp$EraseDocs. "public" "t" (Vector/fromList al "ids" [[2 "y"]]))]
      (t/is (.getCommitted (.executeTx tu/*node* [del erase] (TxOpts.)))))

    (t/is (= [] (xt/q tu/*node* "SELECT a, b FROM t FOR ALL VALID_TIME WHERE b = 'y'")))
    (t/is (= [] (xt/q tu/*node* "SELECT a, b FROM t")))

    (with-open [del (TxOp$DeleteDocs. "public" "t" nil nil (Vector/fromList al "ids" [1]))]
      (t/is (anomalous? [:incorrect :xtdb.tx/key-arity] (.executeTx tu/*node* [del] (TxOpts.)))))))
