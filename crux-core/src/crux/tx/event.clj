(ns ^:no-doc crux.tx.event
  (:require [clojure.spec.alpha :as s]
            [crux.codec :as c])
  (:import java.util.Date))

(def ^:private date? (partial instance? Date))
(def ^:private id? c/valid-id?)

(defmulti tx-event first)

(defmethod tx-event :crux.tx/put [_]
  (s/cat :op #{:crux.tx/put}
         :id id?
         :doc id?
         :start-valid-time (s/? date?)
         :end-valid-time (s/? date?)))

(defmethod tx-event :crux.tx/delete [_]
  (s/cat :op #{:crux.tx/delete}
         :id id?
         :start-valid-time (s/? date?)
         :end-valid-time (s/? date?)))

(defmethod tx-event :crux.tx/cas [_]
  (s/cat :op #{:crux.tx/cas}
         :id id?
         :old-doc (s/nilable id?)
         :new-doc id?
         :at-valid-time (s/? date?)))

(defmethod tx-event :crux.tx/match [_]
  (s/cat :op #{:crux.tx/match}
         :id id?
         :doc (s/nilable id?)
         :at-valid-time (s/? date?)))

;;; We can't remove the previous valid-time range params from here,
;;; if there are still events with these params in Kafka.
;;; This is checked in the KvIndexer.
(defmethod tx-event :crux.tx/evict [_]
  (s/cat :op #{:crux.tx/evict}
         :id id?
         :start-valid-time (s/? date?)
         :end-valid-time (s/? date?)
         :keep-latest? (s/? boolean?)
         :keep-earliest? (s/? boolean?)))

(defmethod tx-event :crux.tx/fn [_]
  (s/cat :op #{:crux.tx/fn}
         :fn-id id?
         :args-doc (s/? id?)))

(s/def ::tx-event (s/multi-spec tx-event first))
(s/def ::tx-events (s/coll-of ::tx-event))
