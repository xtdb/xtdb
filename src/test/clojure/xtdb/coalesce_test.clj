(ns xtdb.coalesce-test
  (:require [clojure.test :as t]
            [xtdb.coalesce :as coalesce]
            [xtdb.test-util :as tu]
            [xtdb.types :as types])
  (:import xtdb.ICursor
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.types.Types$MinorType))

(t/use-fixtures :each tu/with-allocator)

(def ^:private schema (Schema. [(types/->field "foo" (.getType Types$MinorType/VARCHAR) false)]))

(t/deftest test-coalesce
  (letfn [(coalesced-counts [counts]
            (with-open [^ICursor cursor (let [inner (tu/->cursor schema (for [cnt counts]
                                                                          (repeat cnt {:foo "foo"})))]
                                          (try
                                            (-> inner
                                                (coalesce/->coalescing-cursor tu/*allocator*
                                                                              {:pass-through 5, :ideal-min-block-size 10}))
                                            (catch Throwable t
                                              (.close inner)
                                              (throw t))))]
              (mapv count (tu/<-cursor cursor))))]

    (t/is (= [] (coalesced-counts [])))
    (t/is (= [1] (coalesced-counts [1])))
    (t/is (= [6] (coalesced-counts [6])))

    (t/is (= [4 2 12] (coalesced-counts [4 2 3 3 3 3]))
          "after 5, coalesces small blocks")

    (t/is (= [4 2 12 6] (coalesced-counts [4 2
                                           3 3 3 3
                                           3 3]))
          "passes through incomplete final block")))
