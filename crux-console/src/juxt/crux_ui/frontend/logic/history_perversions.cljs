(ns juxt.crux-ui.frontend.logic.history-perversions
  (:require [medley.core :as m]))

(defn- simplify-history-entry
  [{:keys [crux.query/doc
           crux.db/valid-time]
    :as entry}]
  (assoc doc :crux.db/valid-time valid-time))

(defn- simplify-entity-history [eid history]
  (map simplify-history-entry history))

(defn calc-entity-time-series [numeric-attrs eid->history-range]
  (m/map-kv-vals simplify-entity-history eid->history-range))
