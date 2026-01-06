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

(t/deftest test-merge-fields
  (t/is (= #xt/field :utf8
           (types/merge-fields #xt/field :utf8 #xt/field :utf8)))

  (t/is (= #xt/field {"a" [:union :utf8 :i64]}
           (types/merge-fields #xt/field {"a" :utf8} #xt/field {"a" :i64})))

  (t/is (= #xt/field [:union :utf8 :i64 :f64]
           (types/merge-fields #xt/field [:union :utf8 :i64]
                               #xt/field :f64)))

  (t/testing "merges list types"
    (t/is (= #xt/field [:list :utf8]
             (types/merge-fields #xt/field [:list :utf8]
                                 #xt/field [:list :utf8])))

    (t/is (= #xt/field [:list [:union :utf8 :i64]]
             (types/merge-fields #xt/field [:list :utf8]
                                 #xt/field [:list :i64])))

    (t/is (= #xt/field [:list [:? :i64]]
             (types/merge-fields #xt/field [:list :null]
                                 #xt/field [:list :i64]))))

  (t/testing "merges struct types"
    (t/is (= #xt/field [:struct {"a" :utf8} {"b" :utf8}]
             (types/merge-fields #xt/field [:struct {"a" :utf8} {"b" :utf8}]
                                 #xt/field [:struct {"a" :utf8} {"b" :utf8}])))

    (t/is (= #xt/field [:struct {"a" :utf8} {"b" [:union :utf8 :i64]}]
             (types/merge-fields #xt/field [:struct {"a" :utf8} {"b" :utf8}]
                                 #xt/field [:struct {"a" :utf8} {"b" :i64}])))

    (t/is (= #xt/field {"a" [:? :struct {"a" :utf8} {"b" [:union :utf8 :i64]}]}
             (types/merge-fields #xt/field {"a" [:union [:? :null] [:struct {"a" :utf8} {"b" :utf8}]]}
                                 #xt/field {"a" [:struct {"a" :utf8} {"b" :i64}]})))

    (t/is (= #xt/field [:struct {"a" [:? :utf8]} {"b" :utf8} {"c" [:? :i64]}]
             (types/merge-fields #xt/field [:struct {"a" :utf8} {"b" :utf8}]
                                 #xt/field [:struct {"b" :utf8} {"c" :i64}])))

    (t/is (= #xt/field [:union :f64 [:struct {"a" [:union :i64 :utf8]}]]
             (types/merge-fields #xt/field [:union :f64 [:struct {"a" :i64}]]
                                 #xt/field [:struct {"a" :utf8}])))

    (t/is (= #xt/field [:struct
                        {"a" [:union :i64 :bool]}
                        {"b" [:union :utf8 [:struct {"c" :utf8} {"d" :utf8}]]}]
             (types/merge-fields #xt/field [:struct {"a" :i64} {"b" [:struct {"c" :utf8} {"d" :utf8}]}]
                                 #xt/field [:struct {"a" :bool} {"b" :utf8}]))))

  (t/testing "null behaviour"
    (t/is (= #xt/field [:? :null]
             (types/merge-fields #xt/field [:? :null])))

    (t/is (= #xt/field [:? :null]
             (types/merge-fields #xt/field [:? :null] #xt/field [:? :null])))

    (t/is (= #xt/field {"a" [:? :i64]}
             (types/merge-fields #xt/field {"a" :null} #xt/field {"a" :i64})))

    (t/is (= #xt/field {"a" [:union :f64 :i64 [:? :null]]}
             (types/merge-fields #xt/field {"a" :f64}
                                 #xt/field {"a" [:? :null]}
                                 #xt/field {"a" :i64})))

    (t/testing "nulls kept within the legs they were originally in"
      (t/is (= #xt/field {"a" [:union :f64 [:? :i64]]}
               (types/merge-fields #xt/field {"a" :f64}
                                   #xt/field {"a" [:? :i64]})))

      (t/is (= #xt/field [:union :f64 :f32 [:? :i64]]
               (types/merge-fields #xt/field [:union :f64 :f32]
                                   #xt/field [:? :i64]))
            "other unions flattened")))

  (t/testing "sets"
    (t/is (= #xt/field [:set :i64]
             (types/merge-fields #xt/field [:set :i64])))

    (t/is (= #xt/field [:set :i64]
             (types/merge-fields #xt/field [:set :i64] #xt/field [:set :i64])))

    (t/is (= #xt/field [:set [:union :utf8 :i64]]
             (types/merge-fields #xt/field [:set :utf8] #xt/field [:set :i64]))))

  (t/testing "no struct squashing"
    (t/is (= #xt/field [:struct {"foo" [:struct {"bibble" :bool}]}]
             (types/merge-fields #xt/field [:struct {"foo" [:struct {"bibble" :bool}]}])))

    (t/is (= #xt/field [:struct {"foo" [:union :utf8 [:struct {"bibble" :bool}]]} {"bar" [:? :i64]}]
             (types/merge-fields #xt/field [:struct {"foo" [:struct {"bibble" :bool}]}]
                                 #xt/field [:struct {"foo" :utf8} {"bar" :i64}])))))

(t/deftest test-npe-on-empty-list-children-4721
  (t/testing "merge fields with empty list children shouldn't throw NPE"
    (let [set-field #xt/field {"a" [:? :set]}
          list-field #xt/field {"b" [:? :list]}]
      (t/is (= #xt/field {"a" [:? :set [:? :null]]}
               (types/merge-fields nil set-field)))
      (t/is (= #xt/field {"b" [:? :list [:? :null]]}
               (types/merge-fields nil list-field))))))

(t/deftest field->col-type-error-on-empty-list-4774
  (t/is (= [:union #{[:list :null] :null}]
           (types/field->col-type #xt/field {"a" [:? :list]})))
  (t/is (= [:union #{[:set :null] :null}]
           (types/field->col-type #xt/field {"b" [:? :set]}))))
