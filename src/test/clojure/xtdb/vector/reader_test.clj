(ns xtdb.vector.reader-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw])
  (:import [org.apache.arrow.vector.complex DenseUnionVector StructVector ListVector]
           (org.apache.arrow.vector.types.pojo FieldType)))

(t/use-fixtures :each tu/with-allocator)

(deftest dynamic-relation-copier-test-different-blocks
  (t/testing "copying rows from different simple col-types from different relations"
    (with-open [rel-wtr1 (vw/->rel-writer tu/*allocator*)
                rel-wtr2 (vw/->rel-writer tu/*allocator*)
                rel-wtr3 (vw/->rel-writer tu/*allocator*)]
      (let [my-column-wtr1 (.colWriter rel-wtr1 "my-column" (FieldType/notNullable #xt.arrow/type :i64))
            my-colun-wtr2 (.colWriter rel-wtr2 "my-column" (FieldType/notNullable #xt.arrow/type :utf8))]
        (.writeLong my-column-wtr1 42)
        (.writeObject my-colun-wtr2 "forty-two"))
      (let [copier1 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr1))
            copier2 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr2))]
        (.copyRow copier1 0)
        (.copyRow copier2 0))
      (t/is (= [{:my-column 42} {:my-column "forty-two"}]
               (vr/rel->rows (vw/rel-wtr->rdr rel-wtr3))))))

  (t/testing "copying rows from different (composite) col-types from different relations"
    (t/testing "structs"
      (with-open [rel-wtr1 (vw/->rel-writer tu/*allocator*)
                  rel-wtr2 (vw/->rel-writer tu/*allocator*)
                  rel-wtr3 (vw/->rel-writer tu/*allocator*)]
        (let [my-column-wtr1 (.colWriter rel-wtr1 "my-column" (FieldType/notNullable #xt.arrow/type :struct))
              my-column-wtr2 (.colWriter rel-wtr2 "my-column" (FieldType/notNullable #xt.arrow/type :struct))]
          (.startStruct my-column-wtr1)
          (-> (.structKeyWriter my-column-wtr1 "foo" (FieldType/notNullable #xt.arrow/type :i64))
              (.writeLong 42))
          (-> (.structKeyWriter my-column-wtr1 "bar" (FieldType/notNullable #xt.arrow/type :utf8))
              (.writeObject "forty-two"))
          (.endStruct my-column-wtr1)
          (.startStruct my-column-wtr2)
          (-> (.structKeyWriter my-column-wtr2 "foo" (FieldType/notNullable #xt.arrow/type :f64))
              (.writeDouble 42.0))
          (->> (.structKeyWriter my-column-wtr2 "toto" (FieldType/notNullable #xt.arrow/type :keyword))
               (vw/write-value! :my-keyword))
          (.endStruct my-column-wtr2))

        (let [copier1 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr1))
              copier2 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr2))]
          (.copyRow copier1 0)
          (.copyRow copier2 0))
        (t/is (= [{:my-column {:foo 42, :bar "forty-two"}}
                  {:my-column {:foo 42.0, :toto :my-keyword}}]
                 (vr/rel->rows (vw/rel-wtr->rdr rel-wtr3))))
        (t/is (= (types/->field "my-column" #xt.arrow/type :union false
                                (types/->field "struct" #xt.arrow/type :struct false
                                               (types/->field "foo" #xt.arrow/type :union false
                                                              (types/col-type->field :i64)
                                                              (types/col-type->field :f64))
                                               (types/->field "bar" #xt.arrow/type :union false
                                                              (types/col-type->field :utf8)
                                                              (types/col-type->field :absent))
                                               (types/->field "toto" #xt.arrow/type :union false
                                                              (types/col-type->field :keyword)
                                                              (types/col-type->field :absent))))
                 (.getField (.colWriter rel-wtr3 "my-column"))))))

    (t/testing "unions"
      (with-open [rel-wtr1 (vw/->rel-writer tu/*allocator*)
                  rel-wtr2 (vw/->rel-writer tu/*allocator*)
                  rel-wtr3 (vw/->rel-writer tu/*allocator*)]
        (let [my-column-wtr1 (.colWriter rel-wtr1 "my-column" (FieldType/notNullable #xt.arrow/type :union))
              my-column-wtr2 (.colWriter rel-wtr2 "my-column" (FieldType/notNullable #xt.arrow/type :union))]
          (-> (.legWriter my-column-wtr1 :foo (FieldType/notNullable #xt.arrow/type :i64))
              (.writeLong 42))
          (-> (.legWriter my-column-wtr1 :bar (FieldType/notNullable #xt.arrow/type :utf8))
              (.writeObject "forty-two"))
          (-> (.legWriter my-column-wtr2 :foo (FieldType/notNullable #xt.arrow/type :i64))
              (.writeLong 42))
          (->> (.legWriter my-column-wtr2 :toto (FieldType/notNullable #xt.arrow/type :keyword))
               (vw/write-value! :my-keyword)))

        (let [copier1 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr1))
              copier2 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr2))]
          (.copyRow copier1 0)
          (.copyRow copier1 1)
          (.copyRow copier2 0)
          (.copyRow copier2 1))
        (t/is (= [{:my-column 42}
                  {:my-column "forty-two"}
                  {:my-column 42}
                  {:my-column :my-keyword}]
                 (vr/rel->rows (vw/rel-wtr->rdr rel-wtr3))))
        (t/is (= (types/->field "my-column" #xt.arrow/type :union false
                                (types/col-type->field "foo" :i64)
                                (types/col-type->field "bar" :utf8)
                                (types/col-type->field "toto" :keyword))
                 (.getField (.colWriter rel-wtr3 "my-column"))))))

    (t/testing "list"
      (with-open [rel-wtr1 (vw/->rel-writer tu/*allocator*)
                  rel-wtr2 (vw/->rel-writer tu/*allocator*)
                  rel-wtr3 (vw/->rel-writer tu/*allocator*)]
        (let [my-column-wtr1 (.colWriter rel-wtr1 "my-column" (FieldType/notNullable #xt.arrow/type :list))
              my-column-wtr2 (.colWriter rel-wtr2 "my-column" (FieldType/notNullable #xt.arrow/type :list))]
          (.startList my-column-wtr1)
          (-> (.listElementWriter my-column-wtr1 (FieldType/notNullable #xt.arrow/type :i64))
              (.writeLong 42))
          (-> (.listElementWriter my-column-wtr1)
              (.writeLong 43))
          (.endList my-column-wtr1)
          (.startList my-column-wtr2)
          (-> (.listElementWriter my-column-wtr2 (FieldType/notNullable #xt.arrow/type :utf8))
              (.writeObject "forty-two"))
          (-> (.listElementWriter my-column-wtr2)
              (.writeObject "forty-three"))
          (.endList my-column-wtr2))

        (let [copier1 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr1))
              copier2 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr2))]
          (.copyRow copier1 0)
          (.copyRow copier2 0))

        (t/is (= [{:my-column [42 43]} {:my-column ["forty-two" "forty-three"]}]
                 (vr/rel->rows (vw/rel-wtr->rdr rel-wtr3))))
        (t/is (= (types/->field "my-column" #xt.arrow/type :union false
                                (types/->field "list" #xt.arrow/type :list false
                                               (types/->field "$data$" #xt.arrow/type :union false
                                                              (types/col-type->field :i64)
                                                              (types/col-type->field :utf8))))
                 (.getField (.colWriter rel-wtr3 "my-column"))))))))

(deftest copying-union-legs-with-different-types-throws
  (with-open [rel-wtr1 (vw/->rel-writer tu/*allocator*)
              rel-wtr2 (vw/->rel-writer tu/*allocator*)
              rel-wtr3 (vw/->rel-writer tu/*allocator*)]
    (-> rel-wtr1
        (.colWriter "my-column" (FieldType/notNullable #xt.arrow/type :union))
        (.legWriter :foo (FieldType/notNullable #xt.arrow/type :i64))
        (.writeLong 42))
    (-> rel-wtr2
        (.colWriter "my-column" (FieldType/notNullable #xt.arrow/type :union))
        (.legWriter :foo (FieldType/notNullable #xt.arrow/type :f64))
        (.writeDouble 42.0))
    (t/is (thrown-with-msg?
           RuntimeException #"Field type mismatch"
           (let [copier1 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr1))
                 copier2 (.rowCopier rel-wtr3 (vw/rel-wtr->rdr rel-wtr2))]
             (.copyRow copier1 0)
             (.copyRow copier2 0))))))

(deftest testing-duv->vec-copying
  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              int-vec (.createVector (types/->field "my-int" #xt.arrow/type :i64 false) tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          int-wrt (vw/->writer int-vec)]
      (-> duv-wrt
          (.legWriter #xt.arrow/type :i64)
          (doto (.writeLong 42))
          (doto (.writeLong 43)))

      (let [copier (.rowCopier int-wrt duv)]
        (.copyRow copier 0)
        (.copyRow copier 1))
      (t/is (= [42 43]
               (tu/vec->vals (vw/vec-wtr->rdr  int-wrt)))
            "duv to monomorphic base type vector copying")))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              list-vec (ListVector/empty "my-list" tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          list-wrt (vw/->writer list-vec)]
      (-> duv-wrt
          (.legWriter #xt.arrow/type :i64)
          (doto (.writeLong 42)))
      (t/is (thrown-with-msg?
             RuntimeException
             #"Can not copy from vector of .* to ListVector"
             (.rowCopier list-wrt duv)))))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              list-vec (ListVector/empty "my-list" tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          list-wrt (vw/->writer list-vec)
          duv-list-wrt (.legWriter duv-wrt #xt.arrow/type :list)]
      (.startList duv-list-wrt)
      (-> duv-list-wrt
          (.listElementWriter (FieldType/notNullable #xt.arrow/type :i64))
          (doto (.writeLong 42) (.writeLong 43)))
      (.endList duv-list-wrt)
      (let [copier (.rowCopier list-wrt duv)]
        (.copyRow copier 0))
      (t/is (= [[42 43]]
               (tu/vec->vals (vw/vec-wtr->rdr  list-wrt)))
            "duv to monomorphic list type vector copying")))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              struct-vec (StructVector/empty "my-struct" tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          struct-wrt (vw/->writer struct-vec)]
      (-> duv-wrt
          (.legWriter #xt.arrow/type :i64)
          (doto (.writeLong 42)))
      (t/is (thrown-with-msg?
             RuntimeException
             #"Can not copy from vector of .* to StructVector"
             (.rowCopier struct-wrt duv)))))

  (with-open [duv (DenseUnionVector/empty "my-duv" tu/*allocator*)
              struct-vec (StructVector/empty "my-struct" tu/*allocator*)]
    (let [duv-wrt (vw/->writer duv)
          struct-wrt (vw/->writer struct-vec)
          duv-struct-wrt (.legWriter duv-wrt #xt.arrow/type :struct)]
      (.startStruct duv-struct-wrt)
      (-> (.structKeyWriter duv-struct-wrt "foo" (FieldType/notNullable #xt.arrow/type :i64))
          (.writeLong 42))
      (-> (.structKeyWriter duv-struct-wrt "bar" (FieldType/notNullable #xt.arrow/type :utf8))
          (.writeObject "forty-two"))
      (.endStruct duv-struct-wrt)
      (let [copier (.rowCopier struct-wrt duv)]
        (.copyRow copier 0))
      (t/is (= [{:foo 42, :bar "forty-two"}]
               (tu/vec->vals (vw/vec-wtr->rdr struct-wrt)))
            "duv to monomorphic struct type vector copying"))))
