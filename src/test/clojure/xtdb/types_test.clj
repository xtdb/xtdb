(ns xtdb.types-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]))

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

(t/deftest field->col-type-error-on-empty-list-4774
  (t/is (= [:union #{[:list :null] :null}]
           (types/field->col-type #xt/field {"a" [:? :list]})))
  (t/is (= [:union #{[:set :null] :null}]
           (types/field->col-type #xt/field {"b" [:? :set]}))))
