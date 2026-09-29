(ns xtdb.operator.interrupt-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [xtdb.logical-plan :as lp]
            [xtdb.operator.join :as join]
            [xtdb.test-util :as tu])
  (:import (xtdb.api ICursor)
           (xtdb.arrow RelationReader)))

(t/use-fixtures :each tu/with-allocator)

(def ^:private !pages-pulled (atom 0))

(defmethod lp/ra-expr ::interrupt-after-first-page [_]
  (s/cat :op #{::interrupt-after-first-page}
         :relation ::lp/ra-expression))

(defmethod lp/emit-expr ::interrupt-after-first-page [{:keys [relation]} args]
  (-> (lp/emit-expr relation args)
      (update :->cursor (fn [->inner-cursor]
                          (fn [opts]
                            (let [^ICursor in-cursor (->inner-cursor opts)]
                              (reify ICursor
                                (getCursorType [_] "interrupt-after-first-page")
                                (getChildCursors [_] [])

                                (tryAdvance [_ c]
                                  (.tryAdvance in-cursor
                                               (fn [^RelationReader rel]
                                                 (swap! !pages-pulled inc)
                                                 (.accept c rel)
                                                 (.interrupt (Thread/currentThread)))))

                                (close [_] (.close in-cursor)))))))))

(defn- pages-pulled [plan]
  (reset! !pages-pulled 0)
  (try
    (tu/query-ra plan)
    (catch InterruptedException _)
    (finally
      (Thread/interrupted)))
  @!pages-pulled)

(def ^:private two-pages
  '[::interrupt-after-first-page
    [::tu/pages [[{:a 1}] [{:a 2}]]]])

(t/deftest operators-that-drain-their-input-stop-at-the-next-page-once-interrupted
  (t/is (= 1 (pages-pulled [:group-by '{:columns [{n (row-count)}]} two-pages]))
        "group-by")

  (t/is (= 1 (pages-pulled [:sort '{:order-specs [[a]]} two-pages]))
        "sort")

  (t/is (= 1 (pages-pulled [:join '{:conditions [{a b}]}
                            two-pages
                            '[::tu/pages [[{:b 1}]]]]))
        "hash-join, interrupted on the left")

  (t/is (= 1 (pages-pulled [:join '{:conditions [{b a}]}
                            '[::tu/pages [[{:b 1}]]]
                            two-pages]))
        "hash-join, interrupted on the right")

  (t/is (= 1 (pages-pulled [:let '{:binding-sym X}
                            two-pages
                            '[:relation {:cte-id X :col-names [a]}]]))
        "let, materialising its bound relation")

  (t/is (= 1 (pages-pulled [:apply '{:mode :cross-join, :columns {b ?b}}
                            '[::tu/pages [[{:b 1}]]]
                            two-pages]))
        "apply, draining its dependent relation"))

(t/deftest cross-product-stops-once-interrupted
  (with-open [left (tu/open-rel [{:a 1} {:a 2}])
              right (tu/open-rel [{:b 1} {:b 2}])]
    (.interrupt (Thread/currentThread))
    (try
      (t/is (thrown? InterruptedException (#'join/cross-product left right)))
      (finally
        (Thread/interrupted)))))
