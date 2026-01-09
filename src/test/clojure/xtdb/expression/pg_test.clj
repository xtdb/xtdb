(ns xtdb.expression.pg-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.error]
            [xtdb.test-util :as tu])
  (:import (xtdb.types Oid RegClass RegProc)))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-oid
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES (1)"]])

  (t/is (= [{:v (Oid. 12345)}]
           (xt/q tu/*node* "SELECT 12345::oid v"))
        "int -> oid")

  (t/is (= [{:v 12345}]
           (xt/q tu/*node* "SELECT 12345::oid::int v"))
        "oid -> int")

  (t/is (= [{:v (Oid. 357712798)}]
           (xt/q tu/*node* "SELECT 'foo'::regclass::oid v"))
        "regclass -> oid")

  (t/is (= [{:v (Oid. 1989914641)}]
           (xt/q tu/*node* "SELECT 'array_in'::regproc::oid v"))
        "regproc -> oid")

  (t/is (= [{:v true}]
           (xt/q tu/*node* "SELECT 12345::oid = 12345::oid v"))
        "oid equality"))

(t/deftest test-col-description
  (t/testing "col_description returns NULL (stub - XTDB doesn't support comments yet)"
    (t/is (= [{}]
             (xt/q tu/*node* "SELECT COL_DESCRIPTION(12345, 1) AS descr")))
    (t/is (= [{}]
             (xt/q tu/*node* "SELECT COL_DESCRIPTION(12345::oid, 1) AS descr"))
          "with explicit cast to oid")))

(t/deftest test-regclass
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES (1)"]])

  (t/testing "regclass uses search path to resolve unqualified names"
    (t/is (= [{:v (RegClass. 357712798)}]
             (xt/q tu/*node* "SELECT 'public.foo'::regclass v")
             (xt/q tu/*node* "SELECT 'foo'::regclass v"))
          "text -> regclass"))

  (t/testing "regclass name is returned qualified only if the schema does not appear in search path"
    (t/is (= [{:v "foo"}]
             (xt/q tu/*node* "SELECT 'public.foo'::regclass::varchar v"))
          "text -> regclass -> text")

    (t/is (= [{:v "information_schema.columns"}]
             (xt/q tu/*node* "SELECT 'information_schema.columns'::regclass::varchar v"))
          "text -> regclass -> text"))

  (t/is (= [{:v 357712798}]
           (xt/q tu/*node* "SELECT 'public.foo'::regclass::int v"))
        "text -> regclass -> int")

  (t/is (= [{:v (RegClass. 1151209360)}]
           (xt/q tu/*node* "SELECT 1151209360::regclass v"))
        "int -> regclass")

  (t/is (anomalous? [:incorrect nil
                     #"Relation public.baz does not exist"]
                    (xt/q tu/*node* "SELECT 'public.baz'::regclass")))

  (t/testing "regclass that doesn't match a known table"
    (t/is (= [{:v "999999"}]
             (xt/q tu/*node* "SELECT 999999::regclass::varchar v"))
          "returns a stringified oid/int"))

  (t/is (= '[_id _system_from _system_to _valid_from _valid_to]
           (->> (xt/q tu/*node* "SELECT attname FROM pg_attribute WHERE attrelid = 'public.foo'::regclass ORDER BY attname")
                (mapv (comp symbol :attname)))))

  (t/is (= [{:v true}]
           (xt/q tu/*node* "SELECT 357712798::regclass = 'foo'::regclass v"))
        "regclass with identical oid are equal")

  (t/testing "testing non-literal cast"
    (xt/submit-tx tu/*node* [[:sql "INSERT INTO bar RECORDS {_id: 1, tn: 'foo'}"]])

    (t/is (= [{:v (RegClass. 357712798)}]
             (xt/q tu/*node* "SELECT tn::regclass v FROM bar"))
          "text -> regclass")))

(t/deftest test-regclass-where-expr
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO bar RECORDS {_id: 1}"]
                           [:sql "INSERT INTO bar RECORDS {_id: 2}"]])

  (t/is (= [{:xt/id 2} {:xt/id 1}]
           (xt/q tu/*node* "SELECT _id FROM bar WHERE 'bar'::regclass = 904292726"))))

(t/deftest test-regproc
  (t/is (= [{:v (RegProc. 1989914641)}]
           (xt/q tu/*node* "SELECT 'pg_catalog.array_in'::regproc v")
           (xt/q tu/*node* "SELECT 'array_in'::regproc v"))
        "text -> regproc")

  (t/is (= [{:v (RegProc. 1989914641)}]
           (xt/q tu/*node* "SELECT 1989914641::regproc v"))
        "int -> regproc")

  (t/is (anomalous? [:incorrect nil
                     #"Procedure public.baz does not exist"]
                    (xt/q tu/*node* "SELECT 'public.baz'::regproc")))

  (t/is (= [{:v "999999"}]
           (xt/q tu/*node* "SELECT 999999::regproc::varchar v"))
        "returns a stringified oid/int")

  (t/is (= [{:v true}]
           (xt/q tu/*node* "SELECT 1989914641::regproc = 'array_in'::regproc v"))
        "regproc with identical oid are equal"))

(t/deftest test-regclass-search-path-precedence
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO pg_class RECORDS {_id: 1}"]])

  (t/is (= [{:v (RegClass. 529124840)}]
           (xt/q tu/*node* "SELECT 'pg_class'::regclass v")
           (xt/q tu/*node* "SELECT 'pg_catalog.pg_class'::regclass v"))
        "matches pg_catalog.pg_class over public.pg_class as the former comes first in the search path"))
