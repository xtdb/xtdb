(ns crux.bus-test
  (:require [crux.bus :as bus]
            [clojure.test :as t]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [crux.api :as crux])
  (:import (java.io Closeable)
           (java.time Duration)))

(t/deftest test-bus
  (let [!events (atom [])]
    (with-open [bus ^Closeable (bus/->bus)]
      (bus/send bus {:crux/event-type :foo, :value 1})

      (with-open [_ (bus/listen bus {:crux/event-types #{:foo :baz}} #(swap! !events conj %))]
        (bus/send bus {:crux/event-type :foo, :value 2})
        (bus/send bus {:crux/event-type :bar, :value 1})
        (bus/send bus {:crux/event-type :baz, :value 3}))

      (bus/send bus {:crux/event-type :foo, :value 3})

      ;; just to ensure all the jobs are handled
      ;; - we don't guarantee this if the node is shut down
      (Thread/sleep 100))

    (t/is (= [{:crux/event-type :foo, :value 2}
              {:crux/event-type :baz, :value 3}]
             @!events))))

(defmethod bus/event-spec ::foo [_] (s/keys))
(defmethod bus/event-spec ::bar [_] (s/keys))
(defmethod bus/event-spec ::baz [_] (s/keys))

(t/deftest test-await
  (let [bus (bus/->bus {})]
    (t/testing "ready already"
      (t/is (= ::ready (bus/await bus {:crux/event-types #{::foo}
                                       :->result (fn [] ::ready)}))))

    (t/testing "times out"
      (t/is (= ::timeout (bus/await bus {:crux/event-types #{::foo}
                                         :->result (constantly nil)
                                         :timeout (Duration/ofMillis 10)
                                         :timeout-value ::timeout}))))

    (t/testing "eventually works"
      (future
        (Thread/sleep 100)
        (bus/send bus {:crux/event-type ::foo}))

      (t/is (= ::done (bus/await bus {:crux/event-types #{::foo}
                                      :->result (fn
                                                  ([] nil)
                                                  ([ev] ::done))}))))

    (t/testing "times out if it's not quite ready"
      (let [!latch (promise)]
        (future
          (Thread/sleep 100)
          (bus/send bus {:crux/event-type ::bar}))

        (t/is (= ::timeout (bus/await bus {:crux/event-types #{::bar}
                                           :->result (fn
                                                       ([] nil)
                                                       ([ev] ::done))
                                           :timeout (Duration/ofMillis 10)
                                           :timeout-value ::timeout})))))

    (t/testing "throws if ->result throws on the pool"
      (t/is (thrown-with-msg? Exception #"boom"
                              (bus/await bus {:crux/event-types #{::baz}
                                              :->result (let [caller-thread (Thread/currentThread)]
                                                          (fn []
                                                            (when (not= caller-thread (Thread/currentThread))
                                                              (throw (Exception. "boom")))))})))
      (future
        (Thread/sleep 100)
        (bus/send bus {:crux/event-type ::baz}))

      (t/is (thrown-with-msg? Exception #"boom"
                              (bus/await bus {:crux/event-types #{::baz}
                                              :->result (fn
                                                          ([] nil)
                                                          ([evt] (throw (Exception. "boom"))))}))))))
