(ns core2.types.spec-test
  (:require [clojure.test :as t]
            [core2.types.spec :as ts]
            [clojure.datafy :as cd])
  (:import [org.apache.arrow.vector.types.pojo Field Schema]
           org.apache.arrow.vector.types.Types$MinorType))

(t/deftest can-convert-arrow-type-to-clojure-data-and-back
  (t/is (= (.getType Types$MinorType/INT)
           (ts/->arrow-type {:name "int" :bitWidth 32  :isSigned true})))
  (t/is (= {:name "int" :bitWidth 32 :isSigned true}
           (cd/datafy (.getType Types$MinorType/INT)))))

(t/deftest can-convert-arrow-field-to-clojure-data-and-back
  (t/is (= (Field/nullable "foo" (.getType Types$MinorType/INT))
           (ts/->field {:name "foo" :type {:name "int" :bitWidth 32  :isSigned true} :nullable true :children []})))
  (t/is (= {:name "foo" :type {:name "int" :bitWidth 32  :isSigned true} :nullable true :children []}
           (cd/datafy (Field/nullable "foo" (.getType Types$MinorType/INT))))))

(t/deftest can-convert-arrow-schema-to-clojure-data-and-back
  (t/is (= (Schema. [(Field/nullable "foo" (.getType Types$MinorType/INT))])
           (ts/->schema {:fields [{:name "foo" :type {:name "int" :bitWidth 32  :isSigned true} :nullable true :children []}]})))
  (t/is (= {:fields [{:name "foo" :type {:name "int" :bitWidth 32  :isSigned true} :nullable true :children []}]}
           (cd/datafy (Schema. [(Field/nullable "foo" (.getType Types$MinorType/INT))])))))
