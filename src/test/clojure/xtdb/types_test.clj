(ns xtdb.types-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu]
            [xtdb.types :as types])
  (:import (xtdb.types RegClass RegProc)
           (xtdb.vector.extensions RegClassVector RegProcVector)))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-merge-col-types
  (t/is (= :utf8 (types/merge-col-types :utf8 :utf8)))

  (t/is (= [:union #{:utf8 :i64}]
           (types/merge-col-types :utf8 :i64)))

  (t/is (= [:union #{:utf8 :i64 :f64}]
           (types/merge-col-types [:union #{:utf8 :i64}] :f64)))

  (t/testing "merges list types"
    (t/is (= [:list :utf8]
             (types/merge-col-types [:list :utf8] [:list :utf8])))

    (t/is (= [:list [:union #{:utf8 :i64}]]
             (types/merge-col-types [:list :utf8] [:list :i64])))

    (t/is (= [:list [:union #{:null :i64}]]
             (types/merge-col-types [:list :null] [:list :i64]))))

  (t/testing "merges struct types"
    (t/is (= '[:struct {a :utf8, b :utf8}]
             (types/merge-col-types '[:struct {a :utf8, b :utf8}]
                                    '[:struct {a :utf8, b :utf8}])))

    (t/is (= '[:struct {a :utf8
                        b [:union #{:utf8 :i64}]}]

             (types/merge-col-types '[:struct {a :utf8, b :utf8}]
                                    '[:struct {a :utf8, b :i64}])))

    (t/is (= '[:union #{[:struct {a :utf8, b [:union #{:utf8 :i64}]}] :null}]
             (types/merge-col-types '[:union #{:null [:struct {a :utf8, b :utf8}]}]
                                    '[:struct {a :utf8, b :i64}])))

    (let [struct0 '[:struct {a :utf8, b :utf8}]
          struct1 '[:struct {b :utf8, c :i64}]]
      (t/is (= '[:struct {a [:union #{:utf8 :null}]
                          b :utf8
                          c [:union #{:i64 :null}]}]
               (types/merge-col-types struct0 struct1))))

    (t/is (= '[:union #{:f64 [:struct {a [:union #{:i64 :utf8}]}]}]
             (types/merge-col-types '[:union #{:f64, [:struct {a :i64}]}]
                                    '[:struct {a :utf8}]))))

  (t/testing "null behaviour"
    (t/is (= :null
             (types/merge-col-types :null)))

    (t/is (= :null
             (types/merge-col-types :null :null)))

    (t/is (= [:union #{:null :i64}]
             (types/merge-col-types :null :i64))))

  (t/testing "sets"
    (t/is (= [:set :i64]
             (types/merge-col-types [:set :i64])))

    (t/is (= [:set :i64]
             (types/merge-col-types [:set :i64] [:set :i64])))

    (t/is (= [:set [:union #{:i64 :utf8}]]
             (types/merge-col-types [:set :i64] [:set :utf8]))))


  (t/testing "no struct squashing"
    (t/is (= '[:struct {foo [:struct {bibble :bool}]}]
             (types/merge-col-types '[:struct {foo [:struct {bibble :bool}]}])))))

(t/deftest test-merge-fields
  (t/is (= (types/col-type->field :utf8)
           (types/merge-fields (types/col-type->field :utf8) (types/col-type->field :utf8))))

  (t/is (= #xt/field ["a" :union ["utf8" :utf8] ["i64" :i64]]
           (types/merge-fields #xt/field ["a" :utf8] #xt/field ["a" :i64])))

  (t/is (=
         ;; ordering seems to be important
         ;; (types/col-type->field [:union #{:utf8 :i64 :f64}])
         (types/->field-default-name #xt.arrow/type :union false
                                     [(types/col-type->field :utf8)
                                      (types/col-type->field :i64)
                                      (types/col-type->field :f64)])
         (types/merge-fields (types/col-type->field [:union #{:utf8 :i64}]) (types/col-type->field :f64))))

  (t/testing "merges list types"
    (t/is (= (types/col-type->field [:list :utf8])
             (types/merge-fields (types/col-type->field [:list :utf8])
                                 (types/col-type->field [:list :utf8]))))

    (t/is (= (types/col-type->field [:list [:union #{:utf8 :i64}]])
             (types/merge-fields (types/col-type->field [:list :utf8])
                                 (types/col-type->field [:list :i64]))))

    (t/is (= (types/->field-default-name #xt.arrow/type :list false
                                         [#xt/field ["$data$" :i64 :?]])
             #_(types/col-type->field [:list [:union #{:null :i64}]])
             (types/merge-fields (types/col-type->field [:list :null])
                                 (types/col-type->field [:list :i64])))))

  (t/testing "merges struct types"
    (t/is (= (types/col-type->field '[:struct {a :utf8, b :utf8}])
             (types/merge-fields (types/col-type->field '[:struct {a :utf8, b :utf8}])
                                 (types/col-type->field '[:struct {a :utf8, b :utf8}]))))

    (t/is (= (types/col-type->field '[:struct {a :utf8
                                               b [:union #{:utf8 :i64}]}])
             (types/merge-fields (types/col-type->field '[:struct {a :utf8, b :utf8}])
                                 (types/col-type->field '[:struct {a :utf8, b :i64}]))))

    (t/is (= #xt/field ["struct" :struct :? ["a" :utf8] ["b" :union ["utf8" :utf8] ["i64" :i64]]]
             (types/merge-fields #xt/field ["struct" :union ["null" :null :?] ["struct" :struct ["a" :utf8] ["b" :utf8]]]
                                 #xt/field ["struct" :struct ["a" :utf8] ["b" :i64]])))

    (let [struct0 (types/col-type->field '[:struct {a :utf8, b :utf8}])
          struct1 (types/col-type->field '[:struct {b :utf8, c :i64}])]
      (t/is (= (types/col-type->field '[:struct {a [:union #{:utf8 :null}]
                                                 b :utf8
                                                 c [:union #{:i64 :null}]}])
               (types/merge-fields struct0 struct1))))

    (t/is (= #xt/field ["union" :union ["f64" :f64] ["struct" :struct ["a" :union ["i64" :i64] ["utf8" :utf8]]]]
             (types/merge-fields #xt/field ["union" :union ["f64" :f64] ["struct" :struct ["a" :i64]]]
                                 #xt/field ["struct" :struct ["a" :utf8]])))

    (t/is (= #xt/field ["struct" :struct
                        ["a" :union ["i64" :i64] ["bool" :bool]]
                        ["b" :union
                         ["utf8" :utf8]
                         ["struct" :struct ["c" :utf8], ["d" :utf8]]]]
             (types/merge-fields #xt/field ["struct" :struct
                                            ["a" :i64]
                                            ["b" :struct ["c" :utf8], ["d" :utf8]]]
                                 #xt/field ["struct" :struct ["a" :bool], ["b" :utf8]]))))

  (t/testing "null behaviour"
    (t/is (= (types/col-type->field :null)
             (types/merge-fields (types/col-type->field :null))))

    (t/is (= (types/col-type->field :null)
             (types/merge-fields (types/col-type->field :null) (types/col-type->field :null))))

    (t/is (= #xt/field ["a" :i64 :?]
             (types/merge-fields #xt/field ["a" :null] #xt/field ["a" :i64])))

    (t/is (= #xt/field ["a" :union ["f64" :f64] ["i64" :i64] ["null" :null :?]]
             (types/merge-fields #xt/field ["a" :f64]
                                 #xt/field ["a" :null :?]
                                 #xt/field ["a" :i64])))

    (t/testing "nulls kept within the legs they were originally in"
      (t/is (= #xt/field ["a" :union ["f64" :f64] ["i64" :i64 :?]]
               (types/merge-fields #xt/field ["a" :f64]
                                   #xt/field ["a" :i64 :?])))

      (t/is (= #xt/field ["union" :union ["f64" :f64] ["f32" :f32] ["i64" :i64 :?]]
               (types/merge-fields #xt/field ["union" :union ["f64" :f64] ["f32" :f32]]
                                   #xt/field ["union" :i64 :?]))
            "other unions flattened")))

  (t/testing "sets"
    (t/is (= (types/col-type->field [:set :i64])
             (types/merge-fields (types/col-type->field [:set :i64]))))

    (t/is (= (types/col-type->field [:set :i64])
             (types/merge-fields (types/col-type->field [:set :i64]) (types/col-type->field [:set :i64]))))

    (t/is (= #xt/field ["set" :set ["$data$" :union ["utf8" :utf8] ["i64" :i64]]]
             (types/merge-fields (types/col-type->field [:set :utf8]) (types/col-type->field [:set :i64])))))

  (t/testing "no struct squashing"
    (t/is (= (types/col-type->field '[:struct {foo [:struct {bibble :bool}]}])
             (types/merge-fields #xt/field ["struct" :struct ["foo" :struct ["bibble" :bool]]])))

    (t/is (= #xt/field ["struct" :struct
                        ["foo" :union
                         ["utf8" :utf8]
                         ["struct" :struct ["bibble" :bool]]]
                        ["bar" :i64 :?]]

             (types/merge-fields #xt/field ["struct" :struct ["foo" :struct ["bibble" :bool]]]
                                 #xt/field ["struct" :struct ["foo" :utf8] ["bar" :i64]])))))

(t/deftest test-npe-on-empty-list-children-4721
  (t/testing "merge fields with empty list children shouldn't throw NPE"
    (let [set-field (types/->field "a" #xt.arrow/type :set true)
          list-field (types/->field "b" #xt.arrow/type :list true)]
      (t/is (= (types/->field "a" #xt.arrow/type :set true (types/->field "$data$" #xt.arrow/type :null true)) 
               (types/merge-fields nil set-field)))
      (t/is (= (types/->field "b" #xt.arrow/type :list true (types/->field "$data$" #xt.arrow/type :null true)) 
               (types/merge-fields nil list-field))))))

(t/deftest field->col-type-error-on-empty-list-4774
  (t/is (= [:union #{[:list :null] :null}] (types/field->col-type (types/->field "a" #xt.arrow/type :list true))))
  (t/is (= [:union #{[:set :null] :null}] (types/field->col-type (types/->field "b" #xt.arrow/type :set true)))))
