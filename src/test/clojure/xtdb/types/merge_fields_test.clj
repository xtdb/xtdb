(ns xtdb.types.merge-fields-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu])
  (:import (xtdb.arrow MergeFields)))

(t/use-fixtures :each tu/with-allocator)

(defn- merge-fields [& fields]
  (MergeFields/mergeFields fields))

(t/deftest test-merge-fields
  (t/is (= #xt/field :utf8
           (merge-fields #xt/field :utf8 #xt/field :utf8)))

  (t/is (= #xt/field [:union :utf8 :i64]
           (merge-fields #xt/field {"a" :utf8} #xt/field {"a" :i64})))

  (t/is (= #xt/field [:union :utf8 :i64 :f64]
           (merge-fields #xt/field [:union :utf8 :i64]
                         #xt/field :f64))))

(t/deftest test-merge-fields-lists
  (t/is (= #xt/field [:list :utf8]
           (merge-fields #xt/field [:list :utf8]
                         #xt/field [:list :utf8])))

  (t/is (= #xt/field [:list [:union :utf8 :i64]]
           (merge-fields #xt/field [:list :utf8]
                         #xt/field [:list :i64])))

  (t/is (= #xt/field [:list [:? :i64]]
           (merge-fields #xt/field [:list :null]
                         #xt/field [:list :i64]))))

(t/deftest test-merge-fields-structs
  (t/is (= #xt/field [:struct {"a" :utf8, "b" :utf8}]
           (merge-fields #xt/field [:struct {"a" :utf8, "b" :utf8}]
                         #xt/field [:struct {"a" :utf8, "b" :utf8}])))

  (t/is (= #xt/field [:struct {"a" :utf8, "b" [:union :utf8 :i64]}]
           (merge-fields #xt/field [:struct {"a" :utf8, "b" :utf8}]
                         #xt/field [:struct {"a" :utf8, "b" :i64}])))

  (t/is (= #xt/field [:? :struct {"a" :utf8, "b" [:union :utf8 :i64]}]
           (merge-fields #xt/field {"a" [:union [:? :null] [:struct {"a" :utf8, "b" :utf8}]]}
                         #xt/field {"a" [:struct {"a" :utf8, "b" :i64}]})))

  (t/is (= #xt/field [:struct {"a" [:? :utf8], "b" :utf8, "c" [:? :i64]}]
           (merge-fields #xt/field [:struct {"a" :utf8, "b" :utf8}]
                         #xt/field [:struct {"b" :utf8, "c" :i64}])))

  (t/is (= #xt/field [:union :f64 [:struct {"a" [:union :i64 :utf8]}]]
           (merge-fields #xt/field [:union :f64 [:struct {"a" :i64}]]
                         #xt/field [:struct {"a" :utf8}])))

  (t/is (= #xt/field [:struct {"a" [:union :i64 :bool], "b" [:union :utf8 [:struct {"c" :utf8, "d" :utf8}]]}]
           (merge-fields #xt/field [:struct {"a" :i64, "b" [:struct {"c" :utf8, "d" :utf8}]}]
                         #xt/field [:struct {"a" :bool, "b" :utf8}]))))

(t/deftest test-merge-fields-nulls
  (t/is (= #xt/field [:? :null]
           (merge-fields #xt/field [:? :null])))

  (t/is (= #xt/field [:? :null]
           (merge-fields #xt/field [:? :null] #xt/field [:? :null])))

  (t/is (= #xt/field [:? :i64]
           (merge-fields #xt/field {"a" :null} #xt/field {"a" :i64})))

  (t/is (= #xt/field [:union :f64 :i64 [:? :null]]
           (merge-fields #xt/field {"a" :f64}
                         #xt/field {"a" [:? :null]}
                         #xt/field {"a" :i64})))

  (t/is (= #xt/field [:union :f64 [:? :i64]]
           (merge-fields #xt/field {"a" :f64}
                         #xt/field {"a" [:? :i64]})))

  (t/is (= #xt/field [:union :f64 :f32 [:? :i64]]
           (merge-fields #xt/field [:union :f64 :f32]
                         #xt/field [:? :i64]))
        "other unions flattened"))

(t/deftest test-merge-fields-sets
  (t/is (= #xt/field [:set :i64]
           (merge-fields #xt/field [:set :i64])))

  (t/is (= #xt/field [:set :i64]
           (merge-fields #xt/field [:set :i64] #xt/field [:set :i64])))

  (t/is (= #xt/field [:set [:union :utf8 :i64]]
           (merge-fields #xt/field [:set :utf8] #xt/field [:set :i64]))))

(t/deftest test-merge-fields-no-struct-squashing
  (t/is (= #xt/field [:struct {"foo" [:struct {"bibble" :bool}]}]
           (merge-fields #xt/field [:struct {"foo" [:struct {"bibble" :bool}]}])))

  (t/is (= #xt/field [:struct {"foo" [:union :utf8 [:struct {"bibble" :bool}]], "bar" [:? :i64]}]
           (merge-fields #xt/field [:struct {"foo" [:struct {"bibble" :bool}]}]
                         #xt/field [:struct {"foo" :utf8, "bar" :i64}]))))

(t/deftest test-npe-on-empty-list-children-4721
  (t/testing "merge fields with empty list children shouldn't throw NPE"
    (let [set-field #xt/field {"a" [:? :set]}
          list-field #xt/field {"b" [:? :list]}]
      (t/is (= #xt/field [:? :set [:? :null]]
               (merge-fields nil set-field)))
      (t/is (= #xt/field [:? :list [:? :null]]
               (merge-fields nil list-field))))))
