(ns xtdb.vector.writer-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.vector.writer :as vw])
  (:import (org.apache.arrow.vector.types Types$MinorType)
           (org.apache.arrow.vector.types.pojo FieldType)
           (xtdb.arrow DenseUnionVector ListVector StructVector Vector)))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(deftest adding-legs-to-dense-union
  (with-open [duv (DenseUnionVector. tu/*allocator* "my-duv")]
    (t/is (= #xt/field ["my-duv" :union ["i64" :i64]]

             (-> duv
                 (doto (.writeObject 42))
                 (.getField)))))

  (with-open [duv (DenseUnionVector. tu/*allocator* "my-duv")]
    (let [my-list-wtr (.vectorFor duv "list" (FieldType/notNullable #xt.arrow/type :list))
          my-set-wtr (.vectorFor duv "set" (FieldType/notNullable #xt.arrow/type :set))]

      (t/is (= #xt/field ["my-duv" :union
                          ["list" :list ["$data$" :null]]
                          ["set" :set ["$data$" :null]]]

               (.getField duv))

            "vectorFor creates lists/sets with uninitialized data vectors")

      (.getListElements my-list-wtr (.getFieldType #xt/type :i64))

      (.getListElements my-set-wtr (.getFieldType #xt/type :f64))

      (t/is (= #xt/field ["my-duv" :union
                          ["list" :list ["$data$" :i64]]
                          ["set" :set ["$data$" :f64]]]
               (.getField duv)))

      (.getListElements my-list-wtr (.getFieldType #xt/type :f64))

      (t/is (= #xt/field ["my-duv" :union
                          ["list" :list ["$data$" :union ["i64" :i64] ["f64" :f64]]]
                          ["set" :set ["$data$" :f64]]]
               (.getField duv)))))

  (with-open [duv (DenseUnionVector. tu/*allocator* "my-duv")]
    (let [my-struct-wtr (.vectorFor duv "struct" (FieldType/notNullable #xt.arrow/type :struct))]
      (t/is (= #xt/field ["my-duv" :union ["struct" :struct]]
               (.getField duv)))

      (let [a-wtr (.vectorFor my-struct-wtr "a" (FieldType/notNullable #xt.arrow/type :union))]
        (t/is (= #xt/field ["my-duv" :union ["struct" :struct ["a" :union]]]
                 (.getField duv)))

        (.vectorFor a-wtr "i64" (FieldType/notNullable (.getType Types$MinorType/BIGINT)))

        (t/is (= #xt/field ["my-duv" :union ["struct" :struct ["a" :union ["i64" :i64]]]]
                 (.getField duv)))

        (-> (.vectorFor my-struct-wtr "b" (FieldType/notNullable #xt.arrow/type :union))
            (.vectorFor "f64" (FieldType/notNullable (.getType Types$MinorType/FLOAT8))))

        (.vectorFor a-wtr "utf8" (FieldType/notNullable (.getType Types$MinorType/VARCHAR)))

        (t/is (= #xt/field ["my-duv" :union
                            ["struct" :struct
                             ["a" :union ["i64" :i64] ["utf8" :utf8]]
                             ["b" :union ["f64" :f64]]]]

                 (.getField duv)))))))

(deftest list-writer-data-vec-transition
  (with-open [list-vec (ListVector. tu/*allocator* "my-list" false)]
    (t/testing "null vector initially"
      (t/is (= #xt/field ["my-list" :list ["$data$" :null :?]]
               (.getField list-vec)))

      (t/is (= #xt/field ["my-list" :list ["$data$" :null :?]]
               (-> list-vec
                   (doto (.getListElements))
                   (.getField)))
            "call getListElements initializes it with a null vector")

      (t/is (= #xt/field ["my-list" :list ["$data$" :i64]]
               (-> list-vec
                   (doto (.getListElements (FieldType/notNullable #xt.arrow/type :i64)))
                   (.getField)))
            "asking for an i64 with an empty data vec swaps for a mono")))

  (with-open [list-vec (ListVector. tu/*allocator* "my-list" false)]
    (t/is (= #xt/field ["my-list" :list ["$data$" :i64]]
             (-> list-vec
                 (doto (.getListElements (FieldType/notNullable #xt.arrow/type :i64)))
                 (doto (.getListElements (FieldType/notNullable #xt.arrow/type :i64)))
                 (.getField)))
          "explicit monomorphic :i64 requested")

    (t/is (= #xt/field ["$data$" :i64]
             (-> list-vec
                 (.getListElements)
                 (.getField)))
          "nested field correct")

    (t/is (= #xt/field ["my-list" :list ["$data$" :union ["i64" :i64] ["f64" :f64]]]
             (-> list-vec
                 (doto (.getListElements (FieldType/notNullable #xt.arrow/type :f64)))
                 (.getField)))
          "asking for an f64 promotes"))

  (with-open [list-vec (Vector/open tu/*allocator* #xt/field ["my-list" :list ["$data$" :i64]])]
    (t/testing "already initialized arrow vector"
      (t/is (= #xt/field ["my-list" :list ["$data$" :i64]]
               (-> list-vec
                   (.getField))))

      (t/is (= #xt/field ["$data$" :i64]
               (-> list-vec
                   (.getListElements)
                   (.getField)))
            "call listElementWriter honours the underlying vector")

      (t/is (= #xt/field ["$data$" :i64]
               (-> list-vec
                   (.getListElements (FieldType/notNullable #xt.arrow/type :i64))
                   (.getField)))
            "can ask for :i64")

      (t/is (= #xt/field ["my-list" :list ["$data$" :union ["i64" :i64] ["f64" :f64]]]
               (-> list-vec
                   (doto (.getListElements (FieldType/notNullable #xt.arrow/type :f64)))
                   (.getField)))
            "asking for f64 promotes"))))

(deftest adding-nested-struct-fields-dynamically
  (with-open [struct-vec (StructVector. tu/*allocator* "my-struct" false)]
    (t/is (= #xt/field ["my-struct" :struct]
             (.getField struct-vec)))

    (t/is (= #xt/field ["foo" :null :?]
             (-> struct-vec
                 (.vectorFor "foo" (FieldType/nullable #xt.arrow/type :null))
                 (.getField)))
          "call to vectorFor with a field-type creates the key")

    (t/is (= #xt/field ["my-struct" :struct ["foo" :f64]]
             (-> struct-vec
                 (doto (.vectorFor "foo" (FieldType/notNullable #xt.arrow/type :f64)))
                 (.getField)))
          "new type promotes the struct key")

    (t/is (= #xt/field ["my-struct" :struct ["foo" :f64] ["bar" :i64]]
             (-> struct-vec
                 (doto (.vectorFor "bar" (FieldType/notNullable #xt.arrow/type :i64)))
                 (doto (.vectorFor "bar" (FieldType/notNullable #xt.arrow/type :i64)))
                 (.getField))))

    (t/is (= #xt/field ["bar" :i64]
             (-> struct-vec
                 (.vectorFor "bar" (FieldType/notNullable #xt.arrow/type :i64))
                 (.getField))))

    (t/is (= #xt/field ["bar" :i64]
             (-> struct-vec
                 (.vectorFor "bar")
                 (.getField))))

    (t/is (= #xt/field ["my-struct" :struct ["foo" :f64] ["bar" :union ["i64" :i64] ["f64" :f64]]]
             (-> struct-vec
                 (doto (.vectorFor "bar" (FieldType/notNullable #xt.arrow/type :f64)))
                 (.getField)))))

  (with-open [struct-vec (Vector/open tu/*allocator* #xt/field ["my-struct" :struct ["baz" :i64]])]
    (t/is (= #xt/field ["my-struct" :struct ["baz" :i64]]
             (.getField struct-vec)))

    (t/is (= #xt/field ["baz" :i64]
             (-> struct-vec
                 (.vectorFor "baz")
                 (.getField)))
          "vectorFor should return the writer for the existing field")

    (t/is (= #xt/field ["baz" :i64]
             (-> struct-vec
                 (.vectorFor "baz" (FieldType/notNullable #xt.arrow/type :i64))
                 (.getField)))
          "call to vectorFor with correct field should not throw")

    (t/is (= #xt/field ["my-struct" :struct ["baz" :union ["i64" :i64] ["f64" :f64]]]
             (-> struct-vec
                 (doto (.vectorFor "baz" (FieldType/notNullable #xt.arrow/type :f64)))
                 (.getField)))
          "vectorFor promotes non pre-existing union type")))

(deftest rel-writer-dynamic-testing
  (with-open [rel (tu/open-rel)]
    (t/is (= #xt/field ["my-union" :union]
             (-> rel
                 (.vectorFor "my-union" (FieldType/notNullable #xt.arrow/type :union))
                 (.getField))))

    (t/is (= #xt/field ["my-union" :union]
             (-> rel
                 (.vectorFor "my-union" (FieldType/notNullable #xt.arrow/type :union))
                 (.getField))))

    (t/is (= #xt/field ["my-union" :union ["i64" :i64]]
             (-> rel
                 (.vectorFor "my-union" (FieldType/notNullable #xt.arrow/type :i64))
                 (.getField))))

    (t/is (= #xt/field ["my-i64" :i64]
             (-> rel
                 (doto (.vectorFor "my-i64" (FieldType/notNullable #xt.arrow/type :i64)))
                 (.vectorFor "my-i64" (FieldType/notNullable #xt.arrow/type :i64))
                 (.getField))))

    (t/is (= #xt/field ["my-i64" :union ["i64" :i64] ["f64" :f64]]
             (-> rel
                 (.vectorFor "my-i64" (FieldType/notNullable #xt.arrow/type :f64))
                 (.getField)))))

  (with-open [rel (tu/open-rel)]
    (let [int-wtr (.vectorFor rel "my-int" (FieldType/notNullable #xt.arrow/type :i64))
          _str-wtr (.vectorFor rel "my-str" (FieldType/notNullable #xt.arrow/type :utf8))]
      (.writeObject int-wtr 42)
      (.endRow rel)

      (t/is (= [#xt/field ["my-int" :i64]
                #xt/field ["my-str" :utf8 :?]]
               (.getFields (.getSchema rel)))))))

(deftest rel-writer-dynamic-struct-writing
  (with-open [rel (tu/open-rel)]
    (let [some-nested-structs [{:foo {:bibble true} :bar {:baz -4113466} :flib {:true false}}
                               {:foo {:bibble true}  :bar {:baz 1001}}]
          col-writer (.vectorFor rel "my-column" (FieldType/notNullable #xt.arrow/type :union))
          struct-wtr (.vectorFor col-writer "struct" (FieldType/notNullable #xt.arrow/type :struct))]
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

