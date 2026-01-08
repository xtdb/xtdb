(ns xtdb.vector.writer-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.test-util :as tu])
  (:import (org.apache.arrow.vector.types Types$MinorType)
           (xtdb.arrow DenseUnionVector ListVector StructVector Vector)))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(deftest adding-legs-to-dense-union
  (with-open [duv (DenseUnionVector. tu/*allocator* "my-duv")]
    (t/is (= #xt/type [:union :i64]
             (-> duv
                 (doto (.writeObject 42))
                 (.getType)))))

  (with-open [duv (DenseUnionVector. tu/*allocator* "my-duv")]
    (let [my-list-wtr (.vectorFor duv "list" #xt.arrow/type :list false)
          my-set-wtr (.vectorFor duv "set" #xt.arrow/type :set false)]

      (t/is (= #xt/type [:union [:list :null] [:set :null]]
               (.getType duv))
            "vectorFor creates lists/sets with uninitialized data vectors")

      (.getListElements my-list-wtr #xt.arrow/type :i64 false)

      (.getListElements my-set-wtr #xt.arrow/type :f64 false)

      (t/is (= #xt/type [:union [:list :i64] [:set :f64]]
               (.getType duv)))

      (.getListElements my-list-wtr #xt.arrow/type :f64 false)

      (t/is (= #xt/type [:union [:list [:union :i64 :f64]] [:set :f64]]
               (.getType duv)))))

  (with-open [duv (DenseUnionVector. tu/*allocator* "my-duv")]
    (let [my-struct-wtr (.vectorFor duv "struct" #xt.arrow/type :struct false)]
      (t/is (= #xt/type [:union :struct]
               (.getType duv)))

      (let [a-wtr (.vectorFor my-struct-wtr "a" #xt.arrow/type :union false)]
        (t/is (= #xt/type [:union [:struct {"a" :union}]]
                 (.getType duv)))

        (.vectorFor a-wtr "i64" (.getType Types$MinorType/BIGINT) false)

        (t/is (= #xt/type [:union [:struct {"a" [:union :i64]}]]
                 (.getType duv)))

        (-> (.vectorFor my-struct-wtr "b" #xt.arrow/type :union false)
            (.vectorFor "f64" (.getType Types$MinorType/FLOAT8) false))

        (.vectorFor a-wtr "utf8" (.getType Types$MinorType/VARCHAR) false)

        (t/is (= #xt/type [:union
                           {"struct" [:struct {"a" [:union {"i64" :i64, "utf8" :utf8}],
                                               "b" [:union {"f64" :f64}]}]}]
                 (.getType duv)))))))

(deftest list-writer-data-vec-transition
  (with-open [list-vec (ListVector. tu/*allocator* "my-list" false)]
    (t/testing "null vector initially"
      (t/is (= #xt/type [:list [:? :null]]
               (.getType list-vec)))

      (t/is (= #xt/type [:list [:? :null]]
               (-> list-vec
                   (doto (.getListElements))
                   (.getType)))
            "call getListElements initializes it with a null vector")

      (t/is (= #xt/type [:list :i64]
               (-> list-vec
                   (doto (.getListElements #xt.arrow/type :i64 false))
                   (.getType)))
            "asking for an i64 with an empty data vec swaps for a mono")))

  (with-open [list-vec (ListVector. tu/*allocator* "my-list" false)]
    (t/is (= #xt/type [:list :i64]
             (-> list-vec
                 (doto (.getListElements #xt.arrow/type :i64 false))
                 (doto (.getListElements #xt.arrow/type :i64 false))
                 (.getType)))
          "explicit monomorphic :i64 requested")

    (t/is (= #xt/type :i64
             (-> list-vec
                 (.getListElements)
                 (.getType)))
          "nested field correct")

    (t/is (= #xt/type [:list [:union :i64 :f64]]
             (-> list-vec
                 (doto (.getListElements #xt.arrow/type :f64 false))
                 (.getType)))
          "asking for an f64 promotes"))

  (with-open [list-vec (Vector/open tu/*allocator* #xt/field {"my-list" [:list :i64]})]
    (t/testing "already initialized arrow vector"
      (t/is (= #xt/type [:list :i64]
               (.getType list-vec)))

      (t/is (= #xt/type :i64
               (-> list-vec
                   (.getListElements)
                   (.getType)))
            "call listElementWriter honours the underlying vector")

      (t/is (= #xt/type :i64
               (-> list-vec
                   (.getListElements #xt.arrow/type :i64 false)
                   (.getType)))
            "can ask for :i64")

      (t/is (= #xt/type [:list [:union :i64 :f64]]
               (-> list-vec
                   (doto (.getListElements #xt.arrow/type :f64 false))
                   (.getType)))
            "asking for f64 promotes"))))

(deftest adding-nested-struct-fields-dynamically
  (with-open [struct-vec (StructVector. tu/*allocator* "my-struct" false)]
    (t/is (= #xt/type :struct
             (.getType struct-vec)))

    (t/is (= #xt/type [:? :null]
             (-> struct-vec
                 (.vectorFor "foo" #xt.arrow/type :null true)
                 (.getType)))
          "call to vectorFor with a field-type creates the key")

    (t/is (= #xt/type [:struct {"foo" :f64}]
             (-> struct-vec
                 (doto (.vectorFor "foo" #xt.arrow/type :f64 false))
                 (.getType)))
          "new type promotes the struct key")

    (t/is (= #xt/type [:struct {"foo" :f64, "bar" :i64}]
             (-> struct-vec
                 (doto (.vectorFor "bar" #xt.arrow/type :i64 false))
                 (doto (.vectorFor "bar" #xt.arrow/type :i64 false))
                 (.getType))))

    (t/is (= #xt/type :i64
             (-> struct-vec
                 (.vectorFor "bar" #xt.arrow/type :i64 false)
                 (.getType))))

    (t/is (= #xt/type :i64
             (-> struct-vec
                 (.vectorFor "bar")
                 (.getType))))

    (t/is (= #xt/type [:struct {"foo" :f64, "bar" [:union :i64 :f64]}]
             (-> struct-vec
                 (doto (.vectorFor "bar" #xt.arrow/type :f64 false))
                 (.getType)))))

  (with-open [struct-vec (Vector/open tu/*allocator* #xt/field {"my-struct" [:struct {"baz" :i64}]})]
    (t/is (= #xt/type [:struct {"baz" :i64}]
             (.getType struct-vec)))

    (t/is (= #xt/type :i64
             (-> struct-vec
                 (.vectorFor "baz")
                 (.getType)))
          "vectorFor should return the writer for the existing field")

    (t/is (= #xt/type :i64
             (-> struct-vec
                 (.vectorFor "baz" #xt.arrow/type :i64 false)
                 (.getType)))
          "call to vectorFor with correct field should not throw")

    (t/is (= #xt/type [:struct {"baz" [:union :i64 :f64]}]
             (-> struct-vec
                 (doto (.vectorFor "baz" #xt.arrow/type :f64 false))
                 (.getType)))
          "vectorFor promotes non pre-existing union type")))

(deftest rel-writer-dynamic-testing
  (with-open [rel (tu/open-rel)]
    (t/is (= #xt/type :union
             (-> rel
                 (.vectorFor "my-union" #xt.arrow/type :union false)
                 (.getType))))

    (t/is (= #xt/type :union
             (-> rel
                 (.vectorFor "my-union" #xt.arrow/type :union false)
                 (.getType))))

    (t/is (= #xt/type [:union :i64]
             (-> rel
                 (.vectorFor "my-union" #xt.arrow/type :i64 false)
                 (.getType))))

    (t/is (= #xt/type :i64
             (-> rel
                 (doto (.vectorFor "my-i64" #xt.arrow/type :i64 false))
                 (.vectorFor "my-i64" #xt.arrow/type :i64 false)
                 (.getType))))

    (t/is (= #xt/type [:union :i64 :f64]
             (-> rel
                 (.vectorFor "my-i64" #xt.arrow/type :f64 false)
                 (.getType)))))

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

