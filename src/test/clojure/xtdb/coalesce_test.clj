(ns xtdb.coalesce-test
  (:require [clojure.test :as t]
            [xtdb.coalesce :as coalesce]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import xtdb.ICursor))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-coalesce
  (letfn [(coalesced-counts [counts]
            (with-open [^ICursor cursor (util/with-close-on-catch [inner (tu/->cursor (for [cnt counts]
                                                                                        (repeat cnt {:foo "foo"})))]
                                          (-> inner
                                              (coalesce/->coalescing-cursor tu/*allocator*
                                                                            {:pass-through 5, :ideal-min-page-size 10})))]
              (mapv count (tu/<-cursor cursor))))]

    (t/is (= [] (coalesced-counts [])))
    (t/is (= [1] (coalesced-counts [1])))
    (t/is (= [6] (coalesced-counts [6])))

    (t/is (= [4 2 12] (coalesced-counts [4 2 3 3 3 3]))
          "after 5, coalesces small pages")

    (t/is (= [4 2 12 6] (coalesced-counts [4 2
                                           3 3 3 3
                                           3 3]))
          "passes through incomplete final page")))
