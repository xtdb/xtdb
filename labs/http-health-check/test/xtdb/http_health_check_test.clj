(ns xtdb.http-health-check-test
  (:require  [clojure.test :as t]
             [xtdb.api :as xt]
             [xtdb.bus :as bus]
             [xtdb.http-health-check :as sut])
  (:import java.io.Closeable))

(t/deftest test-bus
  (let [events (atom [])]

    (with-open [bus ^Closeable (bus/->bus {:sync? true})
                _ (bus/->bus-stop {:bus bus})
                _ (bus/listen bus {::xt/event-types #{:xtdb.node/node-closing
                                                      :xtdb.node/slow-query
                                                      :healthz}}
                              #(swap! events conj (@#'sut/process-event %)))]
      (bus/send bus {::xt/event-type :xtdb.node/node-closing})
      (t/is (= 1 (count @events)))
      (let [event (first @events)]
        (t/is (= (set (keys event))
                 #{:clock :event :namespace :timestamp :xtdb.api/event-type}))
        (t/is (= (select-keys event [:event :namespace ::xt/event-type])
                 {:xtdb.api/event-type :internal, :namespace "xtdb.node", :event "node-closing"})))
      (bus/send bus (bus/->event :healthz :xxxx))
      (t/is (= 2 (count @events)))
      (let [event (last @events)]
        (t/is (= (set (keys event))
                 #{:clock :thread :event :namespace :timestamp :xtdb.api/event-type}))
        (t/is (= (select-keys event [:event :namespace ::xt/event-type])
                 {:xtdb.api/event-type :healthz, :namespace "xtdb.http-health-check-test",
                  :event :xxxx}))))))
