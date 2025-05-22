(ns xtdb.vector.writer-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (org.apache.arrow.memory RootAllocator)
           (org.apache.arrow.vector VectorSchemaRoot)
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
           (org.apache.arrow.vector.types Types$MinorType)
           (org.apache.arrow.vector.types.pojo FieldType Schema)))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(deftest adding-legs-to-dense-union
  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)]
    (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                            (types/col-type->field 'i64 :i64))

             (-> (vw/->writer duv)
                 (doto (.writeObject 42))
                 (.getField)))))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)]
    (let [duv-wtr (vw/->writer duv)
          my-list-wtr (.vectorFor duv-wtr "list" (FieldType/notNullable #xt.arrow/type :list))
          my-set-wtr (.vectorFor duv-wtr "set" (FieldType/notNullable #xt.arrow/type :set))]

      (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                              (types/->field "list" #xt.arrow/type :list false
                                             (types/->field "$data$" #xt.arrow/type :null true))

                              (types/->field "set" #xt.arrow/type :set false
                                             ;; FIXME
                                             #_(types/->field "$data$" #xt.arrow/type :null true)))

               (.getField duv-wtr))

            "vectorFor creates lists/sets with uninitialized data vectors")

      (doto (.getListElements my-list-wtr)
        (.vectorFor "i64" (FieldType/notNullable (.getType (types/col-type->field :i64)))))

      (doto (.getListElements my-set-wtr)
        (.vectorFor "f64" (FieldType/notNullable (.getType (types/col-type->field :f64)))))

      (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                              (types/->field "list" #xt.arrow/type :list false
                                             (types/->field "$data$" #xt.arrow/type :union false
                                                            (types/col-type->field :i64)))

                              (types/->field "set" #xt.arrow/type :set false
                                             (types/->field "$data$" #xt.arrow/type :union false
                                                            (types/col-type->field :f64))))
               (.getField duv-wtr)))

      (doto (.getListElements my-list-wtr)
        (.vectorFor "f64" (FieldType/notNullable (.getType (types/col-type->field :f64)))))

      (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                              (types/->field "list" #xt.arrow/type :list false
                                             (types/->field "$data$" #xt.arrow/type :union false
                                                            (types/col-type->field :i64)
                                                            (types/col-type->field :f64)))

                              (types/->field "set" #xt.arrow/type :set false
                                             (types/->field "$data$" #xt.arrow/type :union false
                                                            (types/col-type->field :f64))))
               (.getField duv-wtr)))))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)]
    (let [duv-wtr (vw/->writer duv)
          my-struct-wtr (.vectorFor duv-wtr "struct" (FieldType/notNullable #xt.arrow/type :struct))]
      (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                              (types/->field "struct" #xt.arrow/type :struct false))

               (.getField duv-wtr)))

      (let [a-wtr (.vectorFor my-struct-wtr "a" (FieldType/notNullable #xt.arrow/type :union))]
        (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                                (types/->field "struct" #xt.arrow/type :struct false
                                               (types/->field "a" #xt.arrow/type :union false)))

                 (.getField duv-wtr)))

        (.vectorFor a-wtr "i64" (FieldType/notNullable (.getType Types$MinorType/BIGINT)))

        (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                                (types/->field "struct" #xt.arrow/type :struct false
                                               (types/->field "a" #xt.arrow/type :union false
                                                              (types/->field "i64" (.getType Types$MinorType/BIGINT) false))))

                 (.getField duv-wtr)))

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

                 (.getField duv-wtr)))))))

(deftest list-writer-data-vec-transition
  (with-open [list-vec (ListVector/empty "my-list" tu/*allocator*)]
    (t/testing "null vector initially"
      (let [list-wrt (vw/->writer list-vec)]

        (t/is (= (types/->field "my-list" #xt.arrow/type :list true
                                (types/col-type->field "$data$" :null))
                 (.getField list-wrt)))

        (t/is (= (types/->field "my-list" #xt.arrow/type :list true
                                (types/->field "$data$" #xt.arrow/type :union false))
                 (-> list-wrt
                     (doto (.getListElements))
                     (.getField)))
              "call listElementWriter initializes it with a dense union")
        (t/is (thrown-with-msg? RuntimeException #"Inner vector type mismatch"
                                (-> list-wrt
                                    (doto (.getListElements (FieldType/notNullable #xt.arrow/type :i64)))
                                    (.getField)))
              "can't now ask for a :i64"))))


  (with-open [list-vec (ListVector/empty "my-list" tu/*allocator*)]

    (let [list-wrt (vw/->writer list-vec)]

      (t/is (= (types/->field "my-list" #xt.arrow/type :list true
                              (types/->field "$data$" #xt.arrow/type :i64 false))
               (-> list-wrt
                   (doto (.getListElements (FieldType/notNullable #xt.arrow/type :i64)))
                   (doto (.getListElements (FieldType/notNullable #xt.arrow/type :i64)))
                   (.getField)))
            "explicit monomorphic :i64 requested")

      (t/is (= (types/->field "$data$" #xt.arrow/type :i64 false)
               (-> list-wrt
                   (.getListElements)
                   (.getField)))
            "nested field correct")

      (t/is (thrown-with-msg? RuntimeException #"Inner vector type mismatch"
                              (-> list-wrt
                                  (doto (.getListElements (FieldType/notNullable #xt.arrow/type :f64)))
                                  (.getField)))
            "can't now ask for a :f64")))

  (with-open [list-vec (.createVector (types/col-type->field "my-list" [:list :i64]) tu/*allocator*)]
    (t/testing "already initialized arrow vector"
      (let [list-wrt (vw/->writer list-vec)]

        (t/is (= (types/->field "my-list" #xt.arrow/type :list false
                                (types/->field "$data$" #xt.arrow/type :i64 false))
                 (-> list-wrt
                     (.getField))))

        (t/is (= (types/->field "$data$" #xt.arrow/type :i64 false)
                 (-> list-wrt
                     (.getListElements)
                     (.getField)))
              "call listElementWriter honours the underlying vector")

        (t/is (= (types/->field "$data$" #xt.arrow/type :i64 false)
                 (-> list-wrt
                     (.getListElements (FieldType/notNullable #xt.arrow/type :i64))
                     (.getField)))
              "can ask for :i64")

        (t/is (thrown-with-msg? RuntimeException #"Inner vector type mismatch"
                                (-> list-wrt
                                    (doto (.getListElements (FieldType/notNullable #xt.arrow/type :f64)))
                                    (.getField)))
              "can't ask for :f64")))))

(deftest adding-nested-struct-fields-dynamically
  (with-open [struct-vec (StructVector/empty "my-struct" tu/*allocator*)]
    (let [struct-wtr (vw/->writer struct-vec)]

      (t/is (= (types/->field "my-struct" #xt.arrow/type :struct true)
               (.getField struct-wtr)))

      (t/is (= (types/->field "foo" #xt.arrow/type :null true)
               (-> struct-wtr
                   (.vectorFor "foo" (FieldType/nullable #xt.arrow/type :null))
                   (.getField)))
            "call to vectorFor with a field-type creates the key")

      (t/is (= (types/->field "my-struct" #xt.arrow/type :struct true
                              (types/->field "foo" #xt.arrow/type :f64 false))
               (-> struct-wtr
                   (doto (.vectorFor "foo" (FieldType/notNullable #xt.arrow/type :f64)))
                   (.getField)))
            "new type promotes the struct key")

      (t/is (= (types/->field "my-struct" #xt.arrow/type :struct true
                              (types/->field "bar" #xt.arrow/type :i64 false)
                              (types/->field "foo" #xt.arrow/type :f64 false))
               (-> struct-wtr
                   (doto (.vectorFor "bar" (FieldType/notNullable #xt.arrow/type :i64)))
                   (doto (.vectorFor "bar" (FieldType/notNullable #xt.arrow/type :i64)))
                   (.getField))))

      (t/is (= (types/->field "bar" #xt.arrow/type :i64 false)
               (-> struct-wtr
                   (.vectorFor "bar" (FieldType/notNullable #xt.arrow/type :i64))
                   (.getField))))

      (t/is (= (types/->field "bar" #xt.arrow/type :i64 false)
               (-> struct-wtr
                   (.vectorFor "bar")
                   (.getField))))

      (t/is (= (types/->field "my-struct" #xt.arrow/type :struct true
                              (types/->field "bar" #xt.arrow/type :union false
                                             (types/->field "i64" #xt.arrow/type :i64 false)
                                             (types/->field "f64" #xt.arrow/type :f64 false))
                              (types/->field "foo" #xt.arrow/type :f64 false))
               (-> struct-wtr
                   (doto (.vectorFor "bar" (FieldType/notNullable #xt.arrow/type :f64)))
                   (.getField))))))

  (with-open [struct-vec (.createVector (types/col-type->field "my-struct" '[:struct {baz :i64}]) tu/*allocator*)]
    (let [struct-wtr (vw/->writer struct-vec)]
      (t/is (= (types/->field "my-struct" #xt.arrow/type :struct false
                              (types/->field "baz" #xt.arrow/type :i64 false))
               (.getField struct-wtr)))

      (t/is (= (types/->field "baz" #xt.arrow/type :i64 false)
               (-> struct-wtr
                   (.vectorFor "baz")
                   (.getField)))
            "vectorFor should return the writer for the existing field")

      (t/is (= (types/->field "baz" #xt.arrow/type :i64 false)
               (-> struct-wtr
                   (.vectorFor "baz" (FieldType/notNullable #xt.arrow/type :i64))
                   (.getField)))
            "call to vectorFor with correct field should not throw")

      (t/is (= (types/->field "my-struct" #xt.arrow/type :struct false
                              (types/->field "baz" #xt.arrow/type :union false
                                             (types/->field "i64" #xt.arrow/type :i64 false)
                                             (types/->field "f64" #xt.arrow/type :f64 false)))
               (-> struct-wtr
                   (doto (.vectorFor "baz" (FieldType/notNullable #xt.arrow/type :f64)))
                   (.getField)))
            "vectorFor promotes non pre-existing union type"))))

(deftest rel-writer-dynamic-testing
  (with-open [rel-wtr (vw/->rel-writer tu/*allocator*)]
    (t/is (= (types/->field "my-union" #xt.arrow/type :union false)
             (-> rel-wtr
                 (.vectorFor "my-union" (FieldType/notNullable #xt.arrow/type :union))
                 (.getField))))

    (t/is (= (types/->field "my-union" #xt.arrow/type :union false)
             (-> rel-wtr
                 (.vectorFor "my-union" (FieldType/notNullable #xt.arrow/type :union))
                 (.getField))))

    (t/is (thrown-with-msg? RuntimeException #"Field type mismatch"
                            (-> rel-wtr
                                (.vectorFor "my-union" (FieldType/notNullable #xt.arrow/type :i64))
                                (.getField))))

    (t/is (= (types/->field "my-i64" #xt.arrow/type :i64 false)
             (-> rel-wtr
                 (doto (.vectorFor "my-i64" (FieldType/notNullable #xt.arrow/type :i64)))
                 (.vectorFor "my-i64" (FieldType/notNullable #xt.arrow/type :i64))
                 (.getField))))

    (t/is (thrown-with-msg? RuntimeException #"Field type mismatch"
                            (-> rel-wtr
                                (.vectorFor "my-i64" (FieldType/notNullable #xt.arrow/type :f64))
                                (.getField)))))

  (t/testing "promotion doesn't (yet) work in rel-wtrs"
    (t/is (thrown-with-msg? RuntimeException
                            #"invalid writeObject"
                            (with-open [rel-wtr (vw/->rel-writer tu/*allocator*)]
                              (let [int-wtr (.vectorFor rel-wtr "my-int" (FieldType/notNullable #xt.arrow/type :i64))
                                    _str-wtr (.vectorFor rel-wtr "my-str" (FieldType/notNullable #xt.arrow/type :utf8))]
                                (.writeObject int-wtr 42)
                                (.endRow rel-wtr)))))))

(deftest rel-writer-fixed-schema-testing
  (let [schema (Schema. [(types/->field "my-list" #xt.arrow/type :list false
                                        (types/->field "$data$" #xt.arrow/type :struct true
                                                       (types/col-type->field "my-int" :i64)
                                                       (types/col-type->field "my-string" :utf8)))
                         (types/->field "my-union" #xt.arrow/type :union false
                                        (types/col-type->field "my-double" :f64)
                                        (types/col-type->field "my-temporal-type" types/nullable-temporal-type))])]
    (with-open [root (VectorSchemaRoot/create schema tu/*allocator*)
                rel-wtr (vw/root->writer root)]

      (t/is (= (types/->field "my-list" #xt.arrow/type :list false
                              (types/->field "$data$" #xt.arrow/type :struct true
                                             (types/col-type->field "my-int" :i64)
                                             (types/col-type->field "my-string" :utf8)))
               (-> rel-wtr (.vectorFor "my-list") (.getField))))

      (t/is (= (types/col-type->field "my-int" :i64)
               (-> rel-wtr
                   (.vectorFor "my-list")
                   (.getListElements)
                   (.vectorFor "my-int")
                   (.getField))))

      (t/is (thrown-with-msg? RuntimeException #"missing vector: my-list2"
                              (-> rel-wtr
                                  (.vectorFor "my-list2" (FieldType/notNullable #xt.arrow/type :i64))
                                  (.getField))))

      (t/is (= (types/->field "my-union" #xt.arrow/type :union false
                              (types/col-type->field "my-double" :f64)
                              (types/col-type->field "my-temporal-type" types/nullable-temporal-type))

               (-> rel-wtr (.vectorFor "my-union") (.getField))))

      (t/is (= (types/col-type->field "my-double" :f64)
               (-> rel-wtr
                   (.vectorFor "my-union")
                   (.vectorFor "my-double")
                   (.getField)))))))

(deftest rel-writer-dynamic-struct-writing
  (with-open [rel-wtr (vw/->rel-writer tu/*allocator*)]
    (let [some-nested-structs [{:foo {:bibble true} :bar {:baz -4113466} :flib {:true false}}
                               {:foo {:bibble true}  :bar {:baz 1001}}]
          col-writer (.vectorFor rel-wtr "my-column" (FieldType/notNullable #xt.arrow/type :union))
          struct-wtr (.vectorFor col-writer "struct" (FieldType/notNullable #xt.arrow/type :struct))]
      (.writeObject struct-wtr (first some-nested-structs))
      (.endRow rel-wtr)
      (.writeObject struct-wtr (second some-nested-structs))
      (.endRow rel-wtr)

      (t/is (= [{:my-column {:flib {:true false}, :foo {:bibble true}, :bar {:baz -4113466}}}
                {:my-column {:foo {:bibble true}, :bar {:baz 1001}}}]
               (vr/rel->rows (vw/rel-wtr->rdr rel-wtr)))))))

(deftest round-trips-nested-composite-types-2345
  (let [x [{:a [5], :b 1}
           {:a [12.0], :b 5, :c 1}
           {:b 1.5}]]
    (with-open [rel (tu/open-rel [(tu/open-vec "x" x)])]
      (t/is (= x (mapv :x (vr/rel->rows rel))))))

  (let [x [{:a 42}
           {:a 12.0, :b 5, :c [1 2 3]}
           {:b 10, :c [8 1.5]}
           {:a 15, :b 25}
           10.0]]
    (with-open [rel (tu/open-rel [(tu/open-vec "x" x)])]
      (t/is (= x (mapv :x (vr/rel->rows rel)))))))

(deftest writes-map-vector
  (let [maps [{}
              {"mal" "Malcolm", "jdt" "Jeremy"}
              {"jms" "James"}]]
    (with-open [map-vec (-> (types/->field "x" #xt.arrow/type [:map {:sorted? true}] false
                                           (types/->field "entries" #xt.arrow/type :struct false
                                                          (types/col-type->field "username" :utf8)
                                                          (types/col-type->field "first-name" :utf8)))
                            (.createVector tu/*allocator*))]

      ;; with maps, we write them as [:list [:struct #{k v}]]
      (let [map-wtr (vw/->writer map-vec)
            entry-wtr (.getListElements map-wtr)
            k-wtr (.vectorFor entry-wtr "username")
            v-wtr (.vectorFor entry-wtr "first-name")]
        (doseq [m maps]
          (doseq [[k v] (sort-by key m)]
            (.writeObject k-wtr k)
            (.writeObject v-wtr v)
            (.endStruct entry-wtr))

          (.endList map-wtr))

        (t/is (= maps (tu/vec->vals (vw/vec-wtr->rdr map-wtr))))))))

(t/deftest throws-on-equivalent-ks
  (t/is (anomalous? [:incorrect nil #"key-already-set"]
                    (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id :foo, :_id :bar}]]))))

(deftest test-extension-types-in-struct-transfer-pairs-3305
  (let [field #xt.arrow/field ["toplevel" #xt.arrow/field-type [#xt.arrow/type :struct false]
                               #xt.arrow/field ["data" #xt.arrow/field-type [#xt.arrow/type :struct false]
                                                #xt.arrow/field ["type" #xt.arrow/field-type [#xt.arrow/type :keyword false]]]]
        vs [{:data {:type :foo}}
            {:type :bar}]]
    (with-open [al (RootAllocator.)
                vec (vw/open-vec al field vs)]
      (t/is (= vs (tu/vec->vals (vr/vec->reader vec)))))))
