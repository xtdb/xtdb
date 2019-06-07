(ns crux.tx.event
  (:require [clojure.spec.alpha :as s]
            [crux.codec :as c])
  (:import java.util.Date))

(def ^:private date? (partial instance? Date))

(s/def ::id (s/conformer (comp str c/new-id)))
(s/def ::doc (s/and (s/keys :req [:crux.db/id]) ::id))
(s/def ::old-doc (s/nilable ::doc))
(s/def ::new-doc ::doc)
(s/def ::at-valid-time date?)
(s/def ::start-valid-time date?)
(s/def ::end-valid-time date?)
(s/def ::keep-latest boolean?)
(s/def ::keep-earliest boolean?)

(defmulti tx-event :op)

(defmethod tx-event :crux.tx/put [_] (s/and (s/keys :req-un [::doc]
                                                    :opt-un [::start-valid-time ::end-valid-time])
                                            (s/conformer (juxt :op :id :doc :start-valid-time :end-valid-time))))

(defmethod tx-event :crux.tx/delete [_] (s/and (s/keys :opt-un [::start-valid-time ::end-valid-time])
                                               (s/conformer (juxt :op :id :start-valid-time :end-valid-time))))

(defmethod tx-event :crux.tx/cas [_] (s/and (s/keys :req-un [::new-doc]
                                                    :opt-un [::old-doc ::at-valid-time])
                                            (s/conformer (juxt :op :id :old-doc :new-doc :at-valid-time))))

(defmethod tx-event :crux.tx/evict [_] (s/and (s/keys :opt-un [::start-valid-time ::end-valid-time ::keep-latest? ::keep-earliest?])
                                              (s/conformer (juxt :op :id :start-valid-time :end-valid-time :keep-latest? :keep-earliest?))))


(s/def ::tx-event (s/and (s/keys :req-un [::id])
                         (s/multi-spec tx-event :op)))

(s/def ::tx-events (s/coll-of ::tx-event))

(defn conform-tx-events [tx-ops]
  (s/assert ::tx-events tx-ops)
  (s/conform ::tx-events tx-ops))
