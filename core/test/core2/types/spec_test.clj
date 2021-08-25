(ns core2.types.spec-test
  (:require [clojure.test :as t]
            [core2.types.spec :as ts]
            [clojure.datafy :as cd])
  (:import [org.apache.arrow.vector.types.pojo Field Schema]
           org.apache.arrow.vector.types.Types$MinorType))

(t/deftest can-convert-arrow-type-to-clojure-data-and-back
  (t/is (= (.getType Types$MinorType/INT)
           (ts/->arrow-type {:name :int :bitWidth 32 :isSigned true})))
  (t/is (= {:name :int :bitWidth 32 :isSigned true}
           (cd/datafy (.getType Types$MinorType/INT))))

  (t/testing "enums"
    (t/is (= (.getType Types$MinorType/FLOAT4)
             (ts/->arrow-type {:name :floatingpoint :precision :SINGLE})))
    (t/is (= {:name :floatingpoint :precision :SINGLE}
             (cd/datafy (.getType Types$MinorType/FLOAT4))))))

(t/deftest can-convert-arrow-field-to-clojure-data-and-back
  (t/is (= (Field/nullable "foo" (.getType Types$MinorType/INT))
           (ts/->field {:name "foo" :type {:name :int :bitWidth 32 :isSigned true}})))
  (t/is (= {:name "foo" :type {:name :int :bitWidth 32  :isSigned true} :nullable true}
           (cd/datafy (Field/nullable "foo" (.getType Types$MinorType/INT))))))

(t/deftest can-convert-arrow-schema-to-clojure-data-and-back
  (t/is (= (Schema. [(Field/nullable "foo" (.getType Types$MinorType/INT))])
           (ts/->schema {:fields [{:name "foo" :type {:name :int :bitWidth 32 :isSigned true}}]})))
  (t/is (= {:fields [{:name "foo" :type {:name :int :bitWidth 32  :isSigned true} :nullable true}]}
           (cd/datafy (Schema. [(Field/nullable "foo" (.getType Types$MinorType/INT))])))))

(t/deftest can-infer-kind-of-type
  (t/is (= :arrow.kind/primitive (ts/type-kind {:name :int :bitWidth 32 :isSigned true})))
  (t/is (= :arrow.kind/primitive (ts/type-kind {:name :floatingpoint :precision :DOUBLE})))
  (t/is (= :arrow.kind/struct (ts/type-kind {:name :struct})))
  (t/is (= :arrow.kind/struct (ts/type-kind {:name :map})))
  (t/is (= :arrow.kind/list (ts/type-kind {:name :list})))
  (t/is (= :arrow.kind/list (ts/type-kind {:name :largelist})))
  (t/is (= :arrow.kind/list (ts/type-kind {:name :fixedsizelist})))
  (t/is (= :arrow.kind/union (ts/type-kind {:name :union}))))
