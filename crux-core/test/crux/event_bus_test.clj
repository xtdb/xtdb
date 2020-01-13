(ns crux.event-bus-test
  (:require [crux.event-bus :as sut]
            [clojure.test :as t])
  (:import [crux.event_bus EventBus]))

(t/deftest test-event-bus
  (let [!unfiltered-events (atom [])
        !filtered-events (atom [])]
    (with-open [bus ^EventBus (sut/->EventBus (atom #{}))]
      (sut/send bus {::sut/event-type :foo, :value 1})

      (sut/listen bus {::sut/event-types #{:foo}} #(swap! !filtered-events conj %))
      (sut/listen bus #(swap! !unfiltered-events conj %))

      (sut/send bus {::sut/event-type :foo, :value 2})
      (sut/send bus {::sut/event-type :bar, :value 1})

      ;; just to ensure all the jobs are handled
      ;; - we don't guarantee this if the node is shut down
      (Thread/sleep 100))

    (t/is (= [{::sut/event-type :foo, :value 2}
              {::sut/event-type :bar, :value 1}]
             @!unfiltered-events))

    (t/is (= [{::sut/event-type :foo, :value 2}]
             @!filtered-events))))
