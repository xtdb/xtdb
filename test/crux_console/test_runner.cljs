(ns crux-console.test-runner
  (:require [cljs.test :refer :all]
            [crux-console.query-analysis-test]))


(defn init []
  (run-tests))