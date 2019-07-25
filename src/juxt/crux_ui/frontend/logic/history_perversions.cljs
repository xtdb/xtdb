(ns juxt.crux-ui.frontend.logic.history-perversions
  (:require [medley.core :as m]))

(defn- simplify-history-entry
  [{:keys [crux.query/doc
           crux.db/content-hash
           crux.tx/tx-time
           crux.tx/tx-id
           crux.db/valid-time]
    :as entry}]
  (assoc doc :crux.db/valid-time valid-time
             :crux.tx/tx-time tx-time
             :crux.tx/tx-id tx-id
             :crux.db/content-hash content-hash))

(defn- simplify-entity-history [eid history]
  (map simplify-history-entry history))

(defn calc-entity-time-series [numeric-attrs eid->history-range]
  (m/map-kv-vals simplify-entity-history eid->history-range))
