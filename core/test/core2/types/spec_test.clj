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
  (t/is (= :arrow.kind/map (ts/type-kind {:name :map})))
  (t/is (= :arrow.kind/list (ts/type-kind {:name :list})))
  (t/is (= :arrow.kind/largelist (ts/type-kind {:name :largelist})))
  (t/is (= :arrow.kind/fixedsizelist (ts/type-kind {:name :fixedsizelist})))
  (t/is (= :arrow.kind/union (ts/type-kind {:name :union}))))

(t/deftest can-merge-fields
  (t/testing "primitive"
    (t/is (= {:name "foo" :type {:name :int :bitWidth 32 :isSigned true} :nullable false}
             (ts/merge-fields {:name "foo" :type {:name :int :bitWidth 32 :isSigned true}}
                              {:name "foo" :type {:name :int :bitWidth 32 :isSigned true}})))

    (t/is (= {:name "foo" :type {:name :int :bitWidth 32 :isSigned true} :nullable true}
             (ts/merge-fields {:name "foo" :type {:name :int :bitWidth 32 :isSigned true} :nullable false}
                              {:name "foo" :type {:name :int :bitWidth 32 :isSigned true} :nullable true})))

    (t/is (= {:name "foo"
              :type {:name :union :mode :DENSE}
              :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                         {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}]}
             (ts/merge-fields {:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                              {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}))))

  (t/testing "union"
    (t/is (= {:name "foo"
              :type {:name :union :mode :DENSE}
              :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                         {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}
                         {:name "foo" :type {:name :varbinary}}]}
             (ts/merge-fields {:name "foo"
                               :type {:name :union :mode :DENSE}
                               :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                                          {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}]}
                              {:name "foo" :type {:name :varbinary}})))

    (t/is (= {:name "foo"
              :type {:name :union :mode :DENSE}
              :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                         {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}
                         {:name "foo" :type {:name :varbinary}}]}
             (ts/merge-fields {:name "foo" :type {:name :varbinary}}
                              {:name "foo"
                               :type {:name :union :mode :DENSE}
                               :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                                          {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}]})))

    (t/is (= {:name "foo"
              :type {:name :union :mode :DENSE}
              :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                         {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}]}
             (ts/merge-fields {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}
                              {:name "foo"
                               :type {:name :union :mode :DENSE}
                               :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                                          {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}]})))

    (t/is (= {:name "foo"
              :type {:name :union :mode :DENSE}
              :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                         {:name "foo" :type {:name :varbinary}}
                         {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}]}
             (ts/merge-fields {:name "foo"
                               :type {:name :union :mode :DENSE}
                               :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                                          {:name "foo" :type {:name :varbinary}}]}
                              {:name "foo"
                               :type {:name :union :mode :DENSE}
                               :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                                          {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}]})))

    (t/is (= {:name "foo"
              :type {:name :union :mode :SPARSE}
              :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                         {:name "foo" :type {:name :varbinary}}
                         {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}]}
             (ts/merge-fields {:name "foo"
                               :type {:name :union :mode :SPARSE}
                               :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                                          {:name "foo" :type {:name :varbinary}}]}
                              {:name "foo"
                               :type {:name :union :mode :SPARSE}
                               :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                                          {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}]})))

    (t/is (= {:name "foo"
              :type {:name :union :mode :DENSE}
              :children [{:name "foo"
                          :type {:name :union :mode :DENSE}
                          :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                                     {:name "foo" :type {:name :varbinary}}]}
                         {:name "foo"
                          :type {:name :union :mode :SPARSE}
                          :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                                     {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}]}]}
             (ts/merge-fields {:name "foo"
                               :type {:name :union :mode :DENSE}
                               :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                                          {:name "foo" :type {:name :varbinary}}]}
                              {:name "foo"
                               :type {:name :union :mode :SPARSE}
                               :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                                          {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}]}))))

  (t/testing "list"
    (t/is (= {:name "foo"
              :type {:name :union :mode :DENSE}
              :children [{:name "foo" :type {:name :list} :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}]}
                         {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}]}
             (ts/merge-fields {:name "foo" :type {:name :list} :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}]}
                              {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}})))

    (t/is (= {:name "foo"
              :type {:name :list}
              :children [{:name "foo"
                          :type {:name :union :mode :DENSE}
                          :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}
                                     {:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}]}]}
             (ts/merge-fields {:name "foo" :type {:name :list} :children [{:name "foo" :type {:name :int :bitWidth 64 :isSigned true}}]}
                              {:name "foo" :type {:name :list} :children [{:name "foo" :type {:name :floatingpoint :precision :DOUBLE}}]})))))
