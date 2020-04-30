(ns crux.bus-test
  (:require [crux.bus :as bus]
            [clojure.test :as t]
            [crux.topology :as topo])
  (:import (java.io Closeable)))

(t/deftest test-bus
  (let [!events (atom [])]
    (with-open [bus ^Closeable (topo/start-component bus/bus {} {})]
      (bus/send bus {:crux/event-type :foo, :value 1})

      (with-open [_ (bus/listen bus {:crux/event-type :foo} #(swap! !events conj %))]
        (bus/send bus {:crux/event-type :foo, :value 2})
        (bus/send bus {:crux/event-type :bar, :value 1}))

      (bus/send bus {:crux/event-type :foo, :value 3})

      ;; just to ensure all the jobs are handled
      ;; - we don't guarantee this if the node is shut down
      (Thread/sleep 100))

    (t/is (= [{:crux/event-type :foo, :value 2}]
             @!events))))
