(ns xtdb.vector.writer-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.test-util :as tu])
  (:import (xtdb.arrow DenseUnionVector ListVector StructVector Vector)))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(deftest adding-legs-to-dense-union
  (with-open [duv (DenseUnionVector. tu/*allocator*)]
    (t/is (= #xt/field {"my-duv" [:union :i64]}
             (-> duv
                 (doto (.writeObject 42))
                 (.toField "my-duv")))))

  (with-open [duv (DenseUnionVector. tu/*allocator*)]
    (let [my-list-wtr (.vectorFor duv "list" #xt.arrow/type :list false)
          my-set-wtr (.vectorFor duv "set" #xt.arrow/type :set false)]

      (t/is (= #xt/field {"my-duv" [:union [:list :null] [:set :null]]}
               (.toField duv "my-duv"))
            "vectorFor creates lists/sets with uninitialized data vectors")

      (.getListElements my-list-wtr #xt.arrow/type :i64 false)

      (.getListElements my-set-wtr #xt.arrow/type :f64 false)

      (t/is (= #xt/field {"my-duv" [:union [:list :i64] [:set :f64]]}
               (.toField duv "my-duv")))

      (.getListElements my-list-wtr #xt.arrow/type :f64 false)

      (t/is (= #xt/field {"my-duv" [:union [:list [:union :i64 :f64]] [:set :f64]]}
               (.toField duv "my-duv")))))

  (with-open [duv (DenseUnionVector. tu/*allocator*)]
    (let [my-struct-wtr (.vectorFor duv "struct" #xt.arrow/type :struct false)]
      (t/is (= #xt/field {"my-duv" [:union :struct]}
               (.toField duv "my-duv")))

      (let [a-wtr (.vectorFor my-struct-wtr "a" #xt.arrow/type :union false)]
        (t/is (= #xt/field {"my-duv" [:union [:struct {"a" :union}]]}
                 (.toField duv "my-duv")))

        (.vectorFor a-wtr "i64" #xt.arrow/type :i64 false)

        (t/is (= #xt/field {"my-duv" [:union [:struct {"a" [:union :i64]}]]}
                 (.toField duv "my-duv")))

        (-> (.vectorFor my-struct-wtr "b" #xt.arrow/type :union false)
            (.vectorFor "f64" #xt.arrow/type :f64 false))

        (.vectorFor a-wtr "utf8" #xt.arrow/type :utf8 false)

        (t/is (= #xt/field {"my-duv" [:union
                                      {"struct" [:struct {"a" [:union {"i64" :i64, "utf8" :utf8}],
                                                          "b" [:union {"f64" :f64}]}]}]}
                 (.toField duv "my-duv")))))))

(deftest list-writer-data-vec-transition
  (with-open [list-vec (ListVector. tu/*allocator* false)]
    (t/testing "null vector initially"
      (t/is (= #xt/field {"my-list" [:list [:? :null]]}
               (.toField list-vec "my-list")))

      (t/is (= #xt/field {"my-list" [:list [:? :null]]}
               (-> list-vec
                   (doto (.getListElements))
                   (.toField "my-list")))
            "call getListElements initializes it with a null vector")

      (t/is (= #xt/field {"my-list" [:list :i64]}
               (-> list-vec
                   (doto (.getListElements #xt.arrow/type :i64 false))
                   (.toField "my-list")))
            "asking for an i64 with an empty data vec swaps for a mono")))

  (with-open [list-vec (ListVector. tu/*allocator* false)]
    (t/is (= #xt/field {"my-list" [:list :i64]}
             (-> list-vec
                 (doto (.getListElements #xt.arrow/type :i64 false))
                 (doto (.getListElements #xt.arrow/type :i64 false))
                 (.toField "my-list")))
          "explicit monomorphic :i64 requested")

    (t/is (= #xt/field {"$data$" :i64}
             (-> list-vec
                 (.getListElements)
                 (.toField "$data$")))
          "nested field correct")

    (t/is (= #xt/field {"my-list" [:list [:union :i64 :f64]]}
             (-> list-vec
                 (doto (.getListElements #xt.arrow/type :f64 false))
                 (.toField "my-list")))
          "asking for an f64 promotes"))

  (with-open [list-vec (Vector/open tu/*allocator* #xt/field {"my-list" [:list :i64]})]
    (t/testing "already initialized arrow vector"
      (t/is (= #xt/field {"my-list" [:list :i64]}
               (.toField list-vec "my-list")))

      (t/is (= #xt/field {"$data$" :i64}
               (-> list-vec
                   (.getListElements)
                   (.toField "$data$")))
            "call listElementWriter honours the underlying vector")

      (t/is (= #xt/field {"$data$" :i64}
               (-> list-vec
                   (.getListElements #xt.arrow/type :i64 false)
                   (.toField "$data$")))
            "can ask for :i64")

      (t/is (= #xt/field {"my-list" [:list [:union :i64 :f64]]}
               (-> list-vec
                   (doto (.getListElements #xt.arrow/type :f64 false))
                   (.toField "my-list")))
            "asking for f64 promotes"))))

(deftest adding-nested-struct-fields-dynamically
  (with-open [struct-vec (StructVector. tu/*allocator* false)]
    (t/is (= #xt/field {"my-struct" :struct}
             (.toField struct-vec "my-struct")))

    (t/is (= #xt/field {"foo" [:? :null]}
             (-> struct-vec
                 (.vectorFor "foo" #xt.arrow/type :null true)
                 (.toField "foo")))
          "call to vectorFor with a field-type creates the key")

    (t/is (= #xt/field {"my-struct" [:struct {"foo" :f64}]}
             (-> struct-vec
                 (doto (.vectorFor "foo" #xt.arrow/type :f64 false))
                 (.toField "my-struct")))
          "new type promotes the struct key")

    (t/is (= #xt/field {"my-struct" [:struct {"foo" :f64, "bar" :i64}]}
             (-> struct-vec
                 (doto (.vectorFor "bar" #xt.arrow/type :i64 false))
                 (doto (.vectorFor "bar" #xt.arrow/type :i64 false))
                 (.toField "my-struct"))))

    (t/is (= #xt/field {"bar" :i64}
             (-> struct-vec
                 (.vectorFor "bar" #xt.arrow/type :i64 false)
                 (.toField "bar"))))

    (t/is (= #xt/field {"bar" :i64}
             (-> struct-vec
                 (.vectorFor "bar")
                 (.toField "bar"))))

    (t/is (= #xt/field {"my-struct" [:struct {"foo" :f64, "bar" [:union :i64 :f64]}]}
             (-> struct-vec
                 (doto (.vectorFor "bar" #xt.arrow/type :f64 false))
                 (.toField "my-struct")))))

  (with-open [struct-vec (Vector/open tu/*allocator* #xt/field {"my-struct" [:struct {"baz" :i64}]})]
    (t/is (= #xt/field {"my-struct" [:struct {"baz" :i64}]}
             (.toField struct-vec "my-struct")))

    (t/is (= #xt/field {"baz" :i64}
             (-> struct-vec
                 (.vectorFor "baz")
                 (.toField "baz")))
          "vectorFor should return the writer for the existing field")

    (t/is (= #xt/field {"baz" :i64}
             (-> struct-vec
                 (.vectorFor "baz" #xt.arrow/type :i64 false)
                 (.toField "baz")))
          "call to vectorFor with correct field should not throw")

    (t/is (= #xt/field {"my-struct" [:struct {"baz" [:union :i64 :f64]}]}
             (-> struct-vec
                 (doto (.vectorFor "baz" #xt.arrow/type :f64 false))
                 (.toField "my-struct")))
          "vectorFor promotes non pre-existing union type")))

(deftest rel-writer-dynamic-testing
  (with-open [rel (tu/open-rel)]
    (t/is (= #xt/field {"my-union" :union}
             (-> rel
                 (.vectorFor "my-union" #xt.arrow/type :union false)
                 (.toField "my-union"))))

    (t/is (= #xt/field {"my-union" :union}
             (-> rel
                 (.vectorFor "my-union" #xt.arrow/type :union false)
                 (.toField "my-union"))))

    (t/is (= #xt/field {"my-union" [:union :i64]}
             (-> rel
                 (.vectorFor "my-union" #xt.arrow/type :i64 false)
                 (.toField "my-union"))))

    (t/is (= #xt/field {"my-i64" :i64}
             (-> rel
                 (doto (.vectorFor "my-i64" #xt.arrow/type :i64 false))
                 (.vectorFor "my-i64" #xt.arrow/type :i64 false)
                 (.toField "my-i64"))))

    (t/is (= #xt/field {"my-i64" [:union :i64 :f64]}
             (-> rel
                 (.vectorFor "my-i64" #xt.arrow/type :f64 false)
                 (.toField "my-i64")))))

  (with-open [rel (tu/open-rel)]
    (let [int-wtr (.vectorFor rel "my-int" #xt.arrow/type :i64 false)
          _str-wtr (.vectorFor rel "my-str" #xt.arrow/type :utf8 false)]
      (.writeObject int-wtr 42)
      (.endRow rel)

      (t/is (= [#xt/field {"my-int" :i64}
                #xt/field {"my-str" [:? :utf8]}]
               (.getFields (.getSchema rel)))))))

(deftest rel-writer-dynamic-struct-writing
  (with-open [rel (tu/open-rel)]
    (let [some-nested-structs [{:foo {:bibble true} :bar {:baz -4113466} :flib {:true false}}
                               {:foo {:bibble true}  :bar {:baz 1001}}]
          col-writer (.vectorFor rel "my-column" #xt.arrow/type :union false)
          struct-wtr (.vectorFor col-writer "struct" #xt.arrow/type :struct false)]
      (.writeObject struct-wtr (first some-nested-structs))
      (.endRow rel)
      (.writeObject struct-wtr (second some-nested-structs))
      (.endRow rel)

      (t/is (= [{:my-column {:flib {:true false}, :foo {:bibble true}, :bar {:baz -4113466}}}
                {:my-column {:foo {:bibble true}, :bar {:baz 1001}}}]
               (.getAsMaps rel))))))

(deftest round-trips-nested-composite-types-2345
  (let [x [{:a [5], :b 1}
           {:a [12.0], :b 5, :c 1}
           {:b 1.5}]]
    (with-open [rel (tu/open-rel {:x x})]
      (t/is (= x (mapv :x (.getAsMaps rel))))))

  (let [x [{:a 42}
           {:a 12.0, :b 5, :c [1 2 3]}
           {:b 10, :c [8 1.5]}
           {:a 15, :b 25}
           10.0]]
    (with-open [rel (tu/open-rel {:x x})]
      (t/is (= x (mapv :x (.getAsMaps rel)))))))

