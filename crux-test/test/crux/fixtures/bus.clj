(ns crux.fixtures.bus
  (:require [crux.bus :as bus])
  (:import java.util.Date))

(defn listen! [node & event-types]
  (let [events (atom [])]
    (bus/listen (:bus node) {::bus/event-types (set event-types)} #(swap! events conj %))
    events))

(defn wait-for-bus-event! [events timeout]
  (let [start (Date.)]
    (while (not (seq @events))
      (assert (< (- (.getTime (Date.)) (.getTime start)) timeout) "Failed waiting for bus event")
      (Thread/sleep 500))))
