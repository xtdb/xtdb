(ns crux.tx.event
  (:require [clojure.spec.alpha :as s]
            [crux.codec :as c])
  (:import java.util.Date))

(def ^:private date? (partial instance? Date))
(def ^:private id? (partial satisfies? c/IdToBuffer))

(defmulti tx-event first)

(defmethod tx-event :crux.tx/put [_] (s/cat :op #{:crux.tx/put}
                                            :id id?
                                            :doc id?
                                            :start-valid-time (s/? date?)
                                            :end-valid-time (s/? date?)))

(defmethod tx-event :crux.tx/delete [_] (s/cat :op #{:crux.tx/delete}
                                               :id id?
                                               :start-valid-time (s/? date?)
                                               :end-valid-time (s/? date?)))

(defmethod tx-event :crux.tx/cas [_] (s/cat :op #{:crux.tx/cas}
                                            :id id?
                                            :old-doc (s/nilable id?)
                                            :new-doc id?
                                            :at-valid-time (s/? date?)))

(defmethod tx-event :crux.tx/evict [_] (s/cat :op #{:crux.tx/evict}
                                              :id id?
                                              :start-valid-time (s/? date?)
                                              :end-valid-time (s/? date?)
                                              :keep-latest? (s/? boolean?)
                                              :keep-earliest? (s/? boolean?)))


(s/def ::tx-event (s/multi-spec tx-event first))
(s/def ::tx-events (s/coll-of ::tx-event))

;; todo maybe move this back in tx
(defn tx-ops->tx-events [tx-ops]
  (def t tx-ops)
  (let [tx-events (mapv (fn [[op id & args]]
                          (into [op (str (c/new-id id))]
                                (for [arg args]
                                  (if (map? arg)
                                    (-> arg c/new-id str)
                                    arg))))
                        tx-ops)]
    (s/assert ::tx-events tx-events)
    tx-events))
