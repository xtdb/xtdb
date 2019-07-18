(ns crux-console.query-analysis-test
  (:require [cljs.test :refer :all]
            [juxt.crux-ui.frontend.logic.query-analysis :as qa]))

(def q
  '{:find [e p]
    :where [[e :crux.db/id]
            [e :d/my-attr-one p]
            [e :d/my-attr-two 3]]})


(deftest test-symbol-inference
  (let [res (qa/infer-symbol-attr-map q)]
    (is (= res {'e :crux.db/id, 'p :d/my-attr-one}))))


; (run-tests 'crux-console.query-analysis-test)
