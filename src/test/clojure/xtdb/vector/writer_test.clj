(ns xtdb.vector.writer-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import (org.apache.arrow.memory RootAllocator)
           (org.apache.arrow.vector VectorSchemaRoot)
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
           (org.apache.arrow.vector.types Types$MinorType)
           (org.apache.arrow.vector.types.pojo FieldType Schema)))

(t/use-fixtures :each tu/with-allocator)

(deftest adding-legs-to-dense-union
  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)]
    (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                            (types/col-type->field 'i64 :i64))

             (-> (vw/->writer duv)
                 (doto (.writeObject 42))
                 (.getField)))))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)]
    (let [duv-wtr (vw/->writer duv)
          my-list-wtr (.legWriter duv-wtr #xt.arrow/type :list)
          my-set-wtr (.legWriter duv-wtr #xt.arrow/type :set)]

      (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                              (types/->field "list" #xt.arrow/type :list false
                                             (types/->field "$data$" #xt.arrow/type :null true))

                              (types/->field "set" #xt.arrow/type :set false
                                             (types/->field "$data$" #xt.arrow/type :null true)))

               (.getField duv-wtr))

            "legWriter creates lists/sets with uninitialized data vectors")

      (doto (.listElementWriter my-list-wtr)
        (.legWriter (.getType (types/col-type->field :i64))))

      (doto (.listElementWriter my-set-wtr)
        (.legWriter (.getType (types/col-type->field :f64))))

      (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                              (types/->field "list" #xt.arrow/type :list false
                                             (types/->field "$data$" #xt.arrow/type :union false
                                                            (types/col-type->field :i64)))

                              (types/->field "set" #xt.arrow/type :set false
                                             (types/->field "$data$" #xt.arrow/type :union false
                                                            (types/col-type->field :f64))))
               (.getField duv-wtr)))

      (doto (.listElementWriter my-list-wtr)
        (.legWriter (.getType (types/col-type->field :f64))))

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
          my-struct-wtr (.legWriter duv-wtr #xt.arrow/type :struct)]
      (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                              (types/->field "struct" #xt.arrow/type :struct false))

               (.getField duv-wtr)))

      (let [a-wtr (.structKeyWriter my-struct-wtr "a" (FieldType/notNullable #xt.arrow/type :union))]
        (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                                (types/->field "struct" #xt.arrow/type :struct false
                                               (types/->field "a" #xt.arrow/type :union false)))

                 (.getField duv-wtr)))

        (.legWriter a-wtr (.getType Types$MinorType/BIGINT))

        (t/is (= (types/->field "my-duv" #xt.arrow/type :union false
                                (types/->field "struct" #xt.arrow/type :struct false
                                               (types/->field "a" #xt.arrow/type :union false
                                                              (types/->field "i64" (.getType Types$MinorType/BIGINT) false))))

                 (.getField duv-wtr)))

        (-> (.structKeyWriter my-struct-wtr "b" (FieldType/notNullable #xt.arrow/type :union))
            (.legWriter (.getType Types$MinorType/FLOAT8)))

        (-> a-wtr (.legWriter (.getType Types$MinorType/VARCHAR)))

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
                     (doto (.listElementWriter))
                     (.getField)))
              "call listElementWriter initializes it with a dense union")
        (t/is (thrown-with-msg? RuntimeException #"Inner vector type mismatch"
                                (-> list-wrt
                                    (doto (.listElementWriter (FieldType/notNullable #xt.arrow/type :i64)))
                                    (.getField)))
              "can't now ask for a :i64"))))


  (with-open [list-vec (ListVector/empty "my-list" tu/*allocator*)]

    (let [list-wrt (vw/->writer list-vec)]

      (t/is (= (types/->field "my-list" #xt.arrow/type :list true
                              (types/->field "$data$" #xt.arrow/type :i64 false))
               (-> list-wrt
                   (doto (.listElementWriter (FieldType/notNullable #xt.arrow/type :i64)))
                   (doto (.listElementWriter (FieldType/notNullable #xt.arrow/type :i64)))
                   (.getField)))
            "explicit monomorphic :i64 requested")

      (t/is (= (types/->field "$data$" #xt.arrow/type :i64 false)
               (-> list-wrt
                   (.listElementWriter)
                   (.getField)))
            "nested field correct")

      (t/is (thrown-with-msg? RuntimeException #"Inner vector type mismatch"
                              (-> list-wrt
                                  (doto (.listElementWriter (FieldType/notNullable #xt.arrow/type :f64)))
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
                     (.listElementWriter)
                     (.getField)))
              "call listElementWriter honours the underlying vector")

        (t/is (= (types/->field "$data$" #xt.arrow/type :i64 false)
                 (-> list-wrt
                     (.listElementWriter (FieldType/notNullable #xt.arrow/type :i64))
                     (.getField)))
              "can ask for :i64")

        (t/is (thrown-with-msg? RuntimeException #"Inner vector type mismatch"
                                (-> list-wrt
                                    (doto (.listElementWriter (FieldType/notNullable #xt.arrow/type :f64)))
                                    (.getField)))
              "can't ask for :f64")))))

(deftest adding-nested-struct-fields-dynamically
  (with-open [struct-vec (StructVector/empty "my-struct" tu/*allocator*)]
    (let [struct-wtr (vw/->writer struct-vec)]

      (t/is (= (types/->field "my-struct" #xt.arrow/type :struct true)
               (.getField struct-wtr)))

      (t/is (= (types/->field "my-struct" #xt.arrow/type :struct true
                              (types/->field "foo" #xt.arrow/type :null true))
               (-> struct-wtr
                   (doto (.structKeyWriter "foo"))
                   (.getField)))
            "structKeyWriter creates null vector by default")

      (t/is (= (types/->field "foo" #xt.arrow/type :null true)
               (-> struct-wtr
                   (.structKeyWriter "foo" (FieldType/nullable #xt.arrow/type :null))
                   (.getField)))
            "call to structKeyWriter with same field should not throw")

      (t/is (= (types/->field "my-struct" #xt.arrow/type :struct true
                              (types/->field "foo" #xt.arrow/type :f64 false))
               (-> struct-wtr
                   (doto (.structKeyWriter "foo" (FieldType/notNullable #xt.arrow/type :f64)))
                   (.getField)))
            "new type promotes the struct key")

      (t/is (= (types/->field "my-struct" #xt.arrow/type :struct true
                              (types/->field "bar" #xt.arrow/type :i64 false)
                              (types/->field "foo" #xt.arrow/type :f64 false))
               (-> struct-wtr
                   (doto (.structKeyWriter "bar" (FieldType/notNullable #xt.arrow/type :i64)))
                   (doto (.structKeyWriter "bar" (FieldType/notNullable #xt.arrow/type :i64)))
                   (.getField))))

      (t/is (= (types/->field "bar" #xt.arrow/type :i64 false)
               (-> struct-wtr
                   (.structKeyWriter "bar" (FieldType/notNullable #xt.arrow/type :i64))
                   (.getField))))

      (t/is (= (types/->field "bar" #xt.arrow/type :i64 false)
               (-> struct-wtr
                   (.structKeyWriter "bar")
                   (.getField))))

      (t/is (= (types/->field "my-struct" #xt.arrow/type :struct true
                              (types/->field "bar" #xt.arrow/type :union false
                                             (types/->field "i64" #xt.arrow/type :i64 false)
                                             (types/->field "f64" #xt.arrow/type :f64 false))
                              (types/->field "foo" #xt.arrow/type :f64 false))
               (-> struct-wtr
                   (doto (.structKeyWriter "bar" (FieldType/notNullable #xt.arrow/type :f64)))
                   (.getField))))))

  (with-open [struct-vec (.createVector (types/col-type->field "my-struct" '[:struct {baz :i64}]) tu/*allocator*)]
    (let [struct-wtr (vw/->writer struct-vec)]
      (t/is (= (types/->field "my-struct" #xt.arrow/type :struct false
                              (types/->field "baz" #xt.arrow/type :i64 false))
               (.getField struct-wtr)))

      (t/is (= (types/->field "baz" #xt.arrow/type :i64 false)
               (-> struct-wtr
                   (.structKeyWriter "baz")
                   (.getField)))
            "structKeyWriter should return the writer for the existing field")

      (t/is (= (types/->field "baz" #xt.arrow/type :i64 false)
               (-> struct-wtr
                   (.structKeyWriter "baz" (FieldType/notNullable #xt.arrow/type :i64))
                   (.getField)))
            "call to structKeyWriter with correct field should not throw")

      (t/is (= (types/->field "my-struct" #xt.arrow/type :struct false
                              (types/->field "baz" #xt.arrow/type :union false
                                             (types/->field "i64" #xt.arrow/type :i64 false)
                                             (types/->field "f64" #xt.arrow/type :f64 false)))
               (-> struct-wtr
                   (doto (.structKeyWriter "baz" (FieldType/notNullable #xt.arrow/type :f64)))
                   (.getField)))
            "structKeyWriter promotes non pre-existing union type"))))

(deftest rel-writer-dynamic-testing
  (with-open [rel-wtr (vw/->rel-writer tu/*allocator*)]
    (t/is (= (types/->field "my-union" #xt.arrow/type :union false)
             (-> rel-wtr
                 (.colWriter "my-union")
                 (.getField))))

    (t/is (= (types/->field "my-union" #xt.arrow/type :union false)
             (-> rel-wtr
                 (.colWriter "my-union" (FieldType/notNullable #xt.arrow/type :union))
                 (.getField))))

    (t/is (thrown-with-msg? RuntimeException #"Field type mismatch"
                            (-> rel-wtr
                                (.colWriter "my-union" (FieldType/notNullable #xt.arrow/type :i64))
                                (.getField))))

    (t/is (= (types/->field "my-i64" #xt.arrow/type :i64 false)
             (-> rel-wtr
                 (doto (.colWriter "my-i64" (FieldType/notNullable #xt.arrow/type :i64)))
                 (.colWriter "my-i64" (FieldType/notNullable #xt.arrow/type :i64))
                 (.getField))))

    (t/is (thrown-with-msg? RuntimeException #"Field type mismatch"
                            (-> rel-wtr
                                (.colWriter "my-i64" (FieldType/notNullable #xt.arrow/type :f64))
                                (.getField)))))

  (t/testing "promotion doesn't (yet) work in rel-wtrs"
    (t/is (thrown-with-msg? RuntimeException
                            #"invalid writeObject"
                            (with-open [rel-wtr (vw/->rel-writer tu/*allocator*)]
                              (let [int-wtr (.colWriter rel-wtr "my-int" (FieldType/notNullable #xt.arrow/type :i64))
                                    _str-wtr (.colWriter rel-wtr "my-str" (FieldType/notNullable #xt.arrow/type :utf8))]
                                (.startRow rel-wtr)
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
               (-> rel-wtr (.colWriter "my-list") (.getField))))

      (t/is (= (types/col-type->field "my-int" :i64)
               (-> rel-wtr
                   (.colWriter "my-list")
                   (.listElementWriter)
                   (.structKeyWriter "my-int")
                   (.getField))))

      (t/is (thrown-with-msg? RuntimeException #"Dynamic column creation unsupported in RootWriter"
                              (-> rel-wtr
                                  (.colWriter "my-list2" (FieldType/notNullable #xt.arrow/type :i64))
                                  (.getField))))

      (t/is (= (types/->field "my-union" #xt.arrow/type :union false
                              (types/col-type->field "my-double" :f64)
                              (types/col-type->field "my-temporal-type" types/nullable-temporal-type))

               (-> rel-wtr (.colWriter "my-union") (.getField))))

      (t/is (= (types/col-type->field "my-double" :f64)
               (-> rel-wtr
                   (.colWriter "my-union")
                   (.legWriter "my-double")
                   (.getField)))))))

(deftest rel-writer-dynamic-struct-writing
  (with-open [rel-wtr (vw/->rel-writer tu/*allocator*)]
    (let [some-nested-structs [{:foo {:bibble true} :bar {:baz -4113466} :flib {:true false}}
                               {:foo {:bibble true}  :bar {:baz 1001}}]
          col-writer (.colWriter rel-wtr "my-column")
          struct-wtr (.legWriter col-writer #xt.arrow/type :struct)]
      (.startRow rel-wtr)
      (.writeObject struct-wtr (first some-nested-structs))
      (.endRow rel-wtr)
      (.startRow rel-wtr)
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
            entry-wtr (.listElementWriter map-wtr)
            k-wtr (.structKeyWriter entry-wtr "username")
            v-wtr (.structKeyWriter entry-wtr "first-name")]
        (doseq [m maps]
          (.startList map-wtr)

          (doseq [[k v] (sort-by key m)]
            (.startStruct entry-wtr)
            (.writeObject k-wtr k)
            (.writeObject v-wtr v)
            (.endStruct entry-wtr))

          (.endList map-wtr))

        (t/is (= maps (tu/vec->vals (vw/vec-wtr->rdr map-wtr))))))))

(t/deftest throws-on-equivalent-ks
  (with-open [node (xtn/start-node)]
    (t/is (thrown-with-msg? IllegalArgumentException
                            #"key-already-set"
                            (xt/submit-tx node [[:put-docs :foo {:xt/id :foo, :xt$id :bar}]])))))

(deftest test-extension-types-in-struct-transfer-pairs-3305
  (let [field #xt.arrow/field ["toplevel" #xt.arrow/field-type [#xt.arrow/type :struct false]
                               #xt.arrow/field ["data" #xt.arrow/field-type [#xt.arrow/type :struct false]
                                                #xt.arrow/field ["type" #xt.arrow/field-type [#xt.arrow/type :keyword false]]]]
        vs [{:data {:type :foo}}
            {:type :bar}]]
    (with-open [al (RootAllocator.)
                vec (vw/open-vec al field vs)]
      (t/is (= vs (tu/vec->vals (vr/vec->reader vec)))))))
