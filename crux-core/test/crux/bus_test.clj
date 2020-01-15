(ns crux.bus-test
  (:require [crux.bus :as bus]
            [clojure.test :as t])
  (:import [crux.bus EventBus]))

(t/deftest test-bus
  (let [!unfiltered-events (atom [])
        !filtered-events (atom [])]
    (with-open [bus ^EventBus (bus/->EventBus (atom #{}))]
      (bus/send bus {::bus/event-type :foo, :value 1})

      (bus/listen bus {::bus/event-types #{:foo}} #(swap! !filtered-events conj %))
      (bus/listen bus #(swap! !unfiltered-events conj %))

      (bus/send bus {::bus/event-type :foo, :value 2})
      (bus/send bus {::bus/event-type :bar, :value 1})

      ;; just to ensure all the jobs are handled
      ;; - we don't guarantee this if the node is shut down
      (Thread/sleep 100))

    (t/is (= [{::bus/event-type :foo, :value 2}
              {::bus/event-type :bar, :value 1}]
             @!unfiltered-events))

    (t/is (= [{::bus/event-type :foo, :value 2}]
             @!filtered-events))))
