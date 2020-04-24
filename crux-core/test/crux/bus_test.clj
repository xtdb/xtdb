(ns crux.bus-test
  (:require [crux.bus :as bus]
            [clojure.test :as t])
  (:import [crux.bus EventBus]))

(t/deftest test-bus
  (let [!events (atom [])]
    (with-open [bus ^EventBus (bus/->EventBus (atom #{}))]
      (bus/send bus {:crux/event-type :foo, :value 1})

      (bus/listen bus {:crux/event-type :foo} #(swap! !events conj %))

      (bus/send bus {:crux/event-type :foo, :value 2})
      (bus/send bus {:crux/event-type :bar, :value 1})

      ;; just to ensure all the jobs are handled
      ;; - we don't guarantee this if the node is shut down
      (Thread/sleep 100))

    (t/is (= [{:crux/event-type :foo, :value 2}]
             @!events))))
