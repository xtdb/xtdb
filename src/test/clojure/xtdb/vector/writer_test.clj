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
    (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                            (types/col-type->field 'i64 :i64))

             (-> duv
                 (doto (.writeObject 42))
                 (.getField)))))

  (with-open [duv (DenseUnionVector. tu/*allocator* "my-duv")]
    (let [my-list-wtr (.vectorFor duv "list" (FieldType/notNullable #xt.arrow/type :list))
          my-set-wtr (.vectorFor duv "set" (FieldType/notNullable #xt.arrow/type :set))]

      (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                              (types/->field "list" #xt.arrow/type :list false
                                             (types/->field "$data$" #xt.arrow/type :null true))

                              (types/->field "set" #xt.arrow/type :set false
                                             (types/->field "$data$" #xt.arrow/type :null true)))

               (.getField duv))

            "vectorFor creates lists/sets with uninitialized data vectors")

      (.getListElements my-list-wtr (FieldType/notNullable (.getType (types/col-type->field :i64))))

      (.getListElements my-set-wtr (FieldType/notNullable (.getType (types/col-type->field :f64))))

      (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                              (types/->field "list" #xt.arrow/type :list false
                                             (types/->field "$data$" #xt.arrow/type :i64 false))

                              (types/->field "set" #xt.arrow/type :set false
                                             (types/->field "$data$" #xt.arrow/type :f64 false)))
               (.getField duv)))

      (.getListElements my-list-wtr (FieldType/notNullable (.getType (types/col-type->field :f64))))

      (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                              (types/->field "list" #xt.arrow/type :list false
                                             (types/->field "$data$" #xt.arrow/type :union false
                                                            (types/col-type->field :i64)
                                                            (types/col-type->field :f64)))

                              (types/->field "set" #xt.arrow/type :set false
                                             (types/->field "$data$" #xt.arrow/type :f64 false)))
               (.getField duv)))))

  (with-open [duv (DenseUnionVector. tu/*allocator* "my-duv")]
    (let [my-struct-wtr (.vectorFor duv "struct" (FieldType/notNullable #xt.arrow/type :struct))]
      (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                              (types/->field "struct" #xt.arrow/type :struct false))

               (.getField duv)))

      (let [a-wtr (.vectorFor my-struct-wtr "a" (FieldType/notNullable #xt.arrow/type :union))]
        (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                                (types/->field "struct" #xt.arrow/type :struct false
                                               (types/->field "a" #xt.arrow/type :union false)))

                 (.getField duv)))

        (.vectorFor a-wtr "i64" (FieldType/notNullable (.getType Types$MinorType/BIGINT)))

        (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                                (types/->field "struct" #xt.arrow/type :struct false
                                               (types/->field "a" #xt.arrow/type :union false
                                                              (types/->field "i64" (.getType Types$MinorType/BIGINT) false))))

                 (.getField duv)))

        (-> (.vectorFor my-struct-wtr "b" (FieldType/notNullable #xt.arrow/type :union))
            (.vectorFor "f64" (FieldType/notNullable (.getType Types$MinorType/FLOAT8))))

        (.vectorFor a-wtr "utf8" (FieldType/notNullable (.getType Types$MinorType/VARCHAR)))

        (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                                (types/->field "struct" #xt.arrow/type :struct false
                                               (types/->field "a" #xt.arrow/type :union false
                                                              (types/col-type->field :i64)
                                                              (types/col-type->field :utf8))
                                               (types/->field "b" #xt.arrow/type :union false
                                                              (types/col-type->field :f64))))

                 (.getField duv)))))))

(deftest list-writer-data-vec-transition
  (with-open [list-vec (ListVector. tu/*allocator* "my-list" false)]
    (t/testing "null vector initially"
      (t/is (= (types/->field "my-list" #xt.arrow/type :list false
                              (types/col-type->field "$data$" :null))
               (.getField list-vec)))

      (t/is (= (types/->field "my-list" #xt.arrow/type :list false
                              (types/->field "$data$" #xt.arrow/type :null true))
               (-> list-vec
                   (doto (.getListElements))
                   (.getField)))
            "call getListElements initializes it with a null vector")

      (t/is (= (types/->field "my-list" #xt.arrow/type :list false
                              (types/->field "$data$" #xt.arrow/type :i64 false))
               (-> list-vec
                   (doto (.getListElements (FieldType/notNullable #xt.arrow/type :i64)))
                   (.getField)))
            "asking for an i64 with an empty data vec swaps for a mono")))

  (with-open [list-vec (ListVector. tu/*allocator* "my-list" false)]
    (t/is (= (types/->field "my-list" #xt.arrow/type :list false
                            (types/->field "$data$" #xt.arrow/type :i64 false))
             (-> list-vec
                 (doto (.getListElements (FieldType/notNullable #xt.arrow/type :i64)))
                 (doto (.getListElements (FieldType/notNullable #xt.arrow/type :i64)))
                 (.getField)))
          "explicit monomorphic :i64 requested")

    (t/is (= (types/->field "$data$" #xt.arrow/type :i64 false)
             (-> list-vec
                 (.getListElements)
                 (.getField)))
          "nested field correct")

    (t/is (= (types/->field "my-list" #xt.arrow/type :list false
                            (types/->field "$data$" #xt.arrow/type :union false
                                           (types/->field "i64" #xt.arrow/type :i64 false)
                                           (types/->field "f64" #xt.arrow/type :f64 false)))
             (-> list-vec
                 (doto (.getListElements (FieldType/notNullable #xt.arrow/type :f64)))
                 (.getField)))
          "asking for an f64 promotes"))

  (with-open [list-vec (Vector/fromField tu/*allocator* (types/col-type->field "my-list" [:list :i64]))]
    (t/testing "already initialized arrow vector"
      (t/is (= (types/->field "my-list" #xt.arrow/type :list false
                              (types/->field "$data$" #xt.arrow/type :i64 false))
               (-> list-vec
                   (.getField))))

      (t/is (= (types/->field "$data$" #xt.arrow/type :i64 false)
               (-> list-vec
                   (.getListElements)
                   (.getField)))
            "call listElementWriter honours the underlying vector")

      (t/is (= (types/->field "$data$" #xt.arrow/type :i64 false)
               (-> list-vec
                   (.getListElements (FieldType/notNullable #xt.arrow/type :i64))
                   (.getField)))
            "can ask for :i64")

      (t/is (= (types/->field "my-list" #xt.arrow/type :list false
                              (types/->field "$data$" #xt.arrow/type :union false
                                             (types/->field "i64" #xt.arrow/type :i64 false)
                                             (types/->field "f64" #xt.arrow/type :f64 false)))
               (-> list-vec
                   (doto (.getListElements (FieldType/notNullable #xt.arrow/type :f64)))
                   (.getField)))
            "asking for f64 promotes"))))

(deftest adding-nested-struct-fields-dynamically
  (with-open [struct-vec (StructVector. tu/*allocator* "my-struct" false)]
    (t/is (= (types/->field "my-struct" #xt.arrow/type :struct false)
             (.getField struct-vec)))

    (t/is (= (types/->field "foo" #xt.arrow/type :null true)
             (-> struct-vec
                 (.vectorFor "foo" (FieldType/nullable #xt.arrow/type :null))
                 (.getField)))
          "call to vectorFor with a field-type creates the key")

    (t/is (= (types/->field "my-struct" #xt.arrow/type :struct false
                            (types/->field "foo" #xt.arrow/type :f64 false))
             (-> struct-vec
                 (doto (.vectorFor "foo" (FieldType/notNullable #xt.arrow/type :f64)))
                 (.getField)))
          "new type promotes the struct key")

    (t/is (= (types/->field "my-struct" #xt.arrow/type :struct false
                            (types/->field "foo" #xt.arrow/type :f64 false)
                            (types/->field "bar" #xt.arrow/type :i64 false))
             (-> struct-vec
                 (doto (.vectorFor "bar" (FieldType/notNullable #xt.arrow/type :i64)))
                 (doto (.vectorFor "bar" (FieldType/notNullable #xt.arrow/type :i64)))
                 (.getField))))

    (t/is (= (types/->field "bar" #xt.arrow/type :i64 false)
             (-> struct-vec
                 (.vectorFor "bar" (FieldType/notNullable #xt.arrow/type :i64))
                 (.getField))))

    (t/is (= (types/->field "bar" #xt.arrow/type :i64 false)
             (-> struct-vec
                 (.vectorFor "bar")
                 (.getField))))

    (t/is (= (types/->field "my-struct" #xt.arrow/type :struct false
                            (types/->field "foo" #xt.arrow/type :f64 false)
                            (types/->field "bar" #xt.arrow/type :union false
                                           (types/->field "i64" #xt.arrow/type :i64 false)
                                           (types/->field "f64" #xt.arrow/type :f64 false)))
             (-> struct-vec
                 (doto (.vectorFor "bar" (FieldType/notNullable #xt.arrow/type :f64)))
                 (.getField)))))

  (with-open [struct-vec (Vector/fromField tu/*allocator* (types/col-type->field "my-struct" '[:struct {baz :i64}]))]
    (t/is (= (types/->field "my-struct" #xt.arrow/type :struct false
                            (types/->field "baz" #xt.arrow/type :i64 false))
             (.getField struct-vec)))

    (t/is (= (types/->field "baz" #xt.arrow/type :i64 false)
             (-> struct-vec
                 (.vectorFor "baz")
                 (.getField)))
          "vectorFor should return the writer for the existing field")

    (t/is (= (types/->field "baz" #xt.arrow/type :i64 false)
             (-> struct-vec
                 (.vectorFor "baz" (FieldType/notNullable #xt.arrow/type :i64))
                 (.getField)))
          "call to vectorFor with correct field should not throw")

    (t/is (= (types/->field "my-struct" #xt.arrow/type :struct false
                            (types/->field "baz" #xt.arrow/type :union false
                                           (types/->field "i64" #xt.arrow/type :i64 false)
                                           (types/->field "f64" #xt.arrow/type :f64 false)))
             (-> struct-vec
                 (doto (.vectorFor "baz" (FieldType/notNullable #xt.arrow/type :f64)))
                 (.getField)))
          "vectorFor promotes non pre-existing union type")))

(deftest rel-writer-dynamic-testing
  (with-open [rel (tu/open-rel)]
    (t/is (= (types/->field "my-union" #xt.arrow/type :union false)
             (-> rel
                 (.vectorFor "my-union" (FieldType/notNullable #xt.arrow/type :union))
                 (.getField))))

    (t/is (= (types/->field "my-union" #xt.arrow/type :union false)
             (-> rel
                 (.vectorFor "my-union" (FieldType/notNullable #xt.arrow/type :union))
                 (.getField))))

    (t/is (= (types/->field "my-union" #xt.arrow/type :union false
                            (types/->field "i64" #xt.arrow/type :i64 false))
             (-> rel
                 (.vectorFor "my-union" (FieldType/notNullable #xt.arrow/type :i64))
                 (.getField))))

    (t/is (= (types/->field "my-i64" #xt.arrow/type :i64 false)
             (-> rel
                 (doto (.vectorFor "my-i64" (FieldType/notNullable #xt.arrow/type :i64)))
                 (.vectorFor "my-i64" (FieldType/notNullable #xt.arrow/type :i64))
                 (.getField))))

    (t/is (= (types/->field "my-i64" #xt.arrow/type :union false
                            (types/->field "i64" #xt.arrow/type :i64 false)
                            (types/->field "f64" #xt.arrow/type :f64 false))
             (-> rel
                 (.vectorFor "my-i64" (FieldType/notNullable #xt.arrow/type :f64))
                 (.getField)))))

  (with-open [rel (tu/open-rel)]
    (let [int-wtr (.vectorFor rel "my-int" (FieldType/notNullable #xt.arrow/type :i64))
          _str-wtr (.vectorFor rel "my-str" (FieldType/notNullable #xt.arrow/type :utf8))]
      (.writeObject int-wtr 42)
      (.endRow rel)

      (t/is (= [(types/->field "my-int" #xt.arrow/type :i64 false)
                (types/->field "my-str" #xt.arrow/type :utf8 true)]
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
               (.toMaps rel))))))

(deftest round-trips-nested-composite-types-2345
  (let [x [{:a [5], :b 1}
           {:a [12.0], :b 5, :c 1}
           {:b 1.5}]]
    (with-open [rel (tu/open-rel {:x x})]
      (t/is (= x (mapv :x (.toMaps rel))))))

  (let [x [{:a 42}
           {:a 12.0, :b 5, :c [1 2 3]}
           {:b 10, :c [8 1.5]}
           {:a 15, :b 25}
           10.0]]
    (with-open [rel (tu/open-rel {:x x})]
      (t/is (= x (mapv :x (.toMaps rel)))))))

(deftest writes-map-vector
  (let [maps [{}
              {"mal" "Malcolm", "jdt" "Jeremy"}
              {"jms" "James"}]]
    (with-open [map-vec (Vector/fromField tu/*allocator*
                                          (types/->field "x" #xt.arrow/type [:map {:sorted? true}] false
                                                         (types/->field "entries" #xt.arrow/type :struct false
                                                                        (types/col-type->field "username" :utf8)
                                                                        (types/col-type->field "first-name" :utf8))))]

      ;; with maps, we write them as [:list [:struct #{k v}]]
      (let [entry-wtr (.getListElements map-vec)
            k-wtr (.vectorFor entry-wtr "username")
            v-wtr (.vectorFor entry-wtr "first-name")]
        (doseq [m maps]
          (doseq [[k v] (sort-by key m)]
            (.writeObject k-wtr k)
            (.writeObject v-wtr v)
            (.endStruct entry-wtr))

          (.endList map-vec))

        (t/is (= maps (tu/vec->vals (vw/vec-wtr->rdr map-vec))))))))
