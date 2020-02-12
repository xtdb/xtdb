(ns crux-ui.logic.plotting-data-calc)

; See https://plot.ly/javascript/line-and-scatter/ for docs

(defn auto-name [v]
  (cond-> v (keyword? v) name))

(defn- attr-info-str
  [attr-key
   {:keys
    [crux.db/valid-time
     crux.db/content-hash
     crux.tx/tx-time
     crux.tx/tx-id]
    :as simple-history-entry}]
  (str (auto-name attr-key) ": " (get simple-history-entry attr-key) "<br>"
       "vt: " valid-time "<br>"
       "tt: " tx-time "<br>"
       "tx-id: " tx-id "<br>"
       "hash: " content-hash))

(defn calc-plotly-trace--attr [attr-key eid simple-history]
  {:name (auto-name eid)
   :type "scatter"
   :text (map (partial attr-info-str attr-key) simple-history)
   :x (map :crux.db/valid-time simple-history)
   :y (map attr-key simple-history)})


(defn- tx-info-str
  [{:keys [crux.db/valid-time crux.tx/tx-time crux.tx/tx-id]}]
  (str "vt: " valid-time "<br>"
       "tt: " tx-time "<br>"
       "tx-id" tx-id))

(defn calc-plotly-trace--tx-scatter [eid txes]
  {:name (auto-name eid)
   :mode "markers"
   :type "scatter"
   :text (map tx-info-str txes)
   :x    (map :crux.db/valid-time txes)
   :y    (map :crux.tx/tx-time txes)})
