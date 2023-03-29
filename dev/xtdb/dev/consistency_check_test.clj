(ns xtdb.dev.consistency-check-test
  (:require [clojure.test :as t]
            [xtdb.dev.consistency-check :refer :all]))

(t/deftest noop-transition-test
  (t/is (= [{} {} {}] (take 3 (state-seq noop-model))))
  (t/is (= [[] [] []] (take 3 (tx-seq noop-model))))
  (let [{:keys [consistent, nodes]} (test-model {:model noop-model})]
    (t/is (true? consistent))
    (t/is (every? #(= {} (:observed %)) nodes))))

(t/deftest const-transition-test
  (let [doc {:xt/id :foo, :n 42}
        st {:foo doc}
        tx [[:xtdb.api/put doc]]
        mdl (const-model doc)]
    (t/is (= [{} st st] (take 3 (state-seq mdl))))
    (t/is (= [tx tx tx] (take 3 (tx-seq mdl))))
    (let [{:keys [consistent, nodes]} (test-model {:model (const-model doc)})]
      (t/is (true? consistent))
      (t/is (every? #(= st (:observed %)) nodes)))))

;; cannot afford to test actual behaviour right now, this is better than nothing
(t/deftest counter-model-runs-without-crashing-test
  (test-model {:model (counter-model {})})
  (test-model {:model (counter-model {:non-determinism 0.5})})
  (test-model {:model (counter-model {:counters 16})})

  (with-out-str
    (print-test {:model (counter-model {:non-determinism 0.5})}))

  (with-out-str
    (print-test {:model (counter-model {:non-determinism 0.0})
                 :storage {:doc-store {:fetch-flake {nil 1.0, (Exception. "boom") 0.1}}}
                 :tx-count 100}))

  (with-out-str
    (print-test {:model (counter-model
                          {:counters 16,
                           :non-determinism 0.0})
                 :node-count 4
                 :submit-threads 4
                 :storage {:doc-store {:fetch-flake {nil 1.0, (Exception. "boom") 0.01}
                                       :submit-flake {nil 1.0, (Exception. "boom") 0.01}
                                       :close-flake {nil 1.0, (Exception. "boom") 0.1}}
                           :tx-log {:subscribe-behaviour :poll
                                    :open-flake {nil 1.0, (Exception. "boom") 0.1}
                                    :cursor-flake {nil 1.0, (Exception. "boom") 0.0001}
                                    :cursor-close-flake {nil 1.0, (Exception. "boom") 0.01}
                                    :close-flake {nil 1.0, (Exception. "boom") 0.1}}}
                 :tx-count 100})))
