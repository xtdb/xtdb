(ns juxt.crux-ui.frontend.subs
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.example-queries :as ex]
            [juxt.crux-ui.frontend.logic.plotting-data-calc :as pd]
            [juxt.crux-ui.frontend.logic.query-analysis :as qa]
            [cljs.reader :as reader]
            [juxt.crux-ui.frontend.logic.time :as time]))



; this is to force editor rerenders
(rf/reg-sub :subs.ui/editor-key  (fnil :db.ui/editor-key 0))

(rf/reg-sub :subs.db.ui/output-side-tab
            (fnil :db.ui/output-side-tab
                  {:db.ui/output-side-tab
                   :db.ui.output-tab/table}))
(rf/reg-sub :subs.db.ui/output-main-tab
            (fnil :db.ui/output-main-tab
                  {:db.ui/output-main-tab
                   :db.ui.output-tab/table}))

(rf/reg-sub
  :subs.ui/root-tab
  (fn [db]
    (case js/location.pathname
      "/query-perf" :db.ui.root-tab/query-perf
      (:db.ui/root-tab db))))



; query related root subscriptions
(rf/reg-sub :subs.query/stats  (fnil :db.meta/stats {}))
(rf/reg-sub :subs.query/input-committed  (fnil :db.query/input-committed  {}))
(rf/reg-sub :subs.query/time  #(:db.query/time % {}))
(rf/reg-sub :subs.query/input  (fnil :db.query/input  {}))
; (rf/reg-sub :subs.query/examples-imported (fnil :db.ui.examples/imported false))
(rf/reg-sub :subs.query/result (fnil :db.query/result {}))
(rf/reg-sub :subs.query/error  #(:db.query/error % false))
(rf/reg-sub :subs.query/analysis-committed (fnil :db.query/analysis-committed {}))
(rf/reg-sub :subs.query/result-analysis (fnil :db.query/result-analysis {}))

; returns a map entity-id->simple-histories
(rf/reg-sub :subs.query/entities-simple-histories (fnil :db.query/eid->simple-history {}))
(rf/reg-sub :subs.query/entities-txes (fnil :db.query/histories {}))


; query derivatives
(rf/reg-sub
  :subs.query/input-edn-committed
  :<- [:subs.query/input-committed]
  qa/try-read-string)

(rf/reg-sub
  :subs.query/input-edn
  :<- [:subs.query/input]
  qa/try-read-string)

(rf/reg-sub
  :subs.query.time/vt
  :<- [:subs.query/time]
  #(:time/vt % (js/Date.)))

(rf/reg-sub
  :subs.query.time/tt
  :<- [:subs.query/time]
  #(:time/tt % (js/Date.)))

(rf/reg-sub
  :subs.query.time/vtc
  :<- [:subs.query.time/vt]
  time/date->comps)

(rf/reg-sub
  :subs.query.time/ttc
  :<- [:subs.query.time/tt]
  time/date->comps)

(rf/reg-sub
  :subs.query/input-malformed?
  :<- [:subs.query/input-edn]
  #(:error % false))

(rf/reg-sub
  :subs.query/analysis
  :<- [:subs.query/input-edn]
  (fn [input-edn]
    (cond
      (not input-edn) nil
      (:error input-edn) nil
      :else (qa/analyse-query input-edn))))

(rf/reg-sub
  :subs.query/headers
  :<- [:subs.query/result]
  :<- [:subs.query/analysis-committed]
  (fn [[q-res q-info]]
    (when q-info
      (if (:full-results? q-info)
         (qa/analyze-full-results-headers q-res)
         (:find q-info)))))

(rf/reg-sub
  :subs.query/results-table
  :<- [:subs.query/headers]
  :<- [:subs.query/analysis-committed]
  :<- [:subs.query/result]
  (fn [[q-headers q-info q-res :as args]]
    (when (every? some? args)
      {:headers q-headers
       :rows (if (:full-results? q-info)
               (->> q-res
                    (map first)
                    (map #(map % q-headers)))
               q-res)})))

(rf/reg-sub
  :subs.query/attr-stats-table
  :<- [:subs.query/stats]
  (fn [stats]
    {:headers [:attribute :frequency]
     :rows (if-not (map? stats)
             []
             (into [[:crux.db/id (:crux.db/id stats)]]
                   (dissoc stats :crux.db/id)))}))


(rf/reg-sub
  :subs.output/tx-history-plot-data
  :<- [:subs.query/entities-txes]
  (fn [eids->txes]
    (if eids->txes
      (map (fn [[k v]] (pd/calc-plotly-trace--tx-scatter k v)) eids->txes))))

(rf/reg-sub
  :subs.query/attr-history-plot-data
  :<- [:subs.query/result-analysis]
  :<- [:subs.query/entities-simple-histories]
  (fn [[result-analysis eids->simple-history]]
    (let [first-numeric (first (:ra/numeric-attrs result-analysis))]
      (if (and first-numeric (map? eids->simple-history))
        {:attribute first-numeric
         :traces (map (fn [[k v]] (pd/calc-plotly-trace--attr first-numeric k v)) eids->simple-history)}))))

(rf/reg-sub
  :subs.query/examples
  (fn [{:db.ui.examples/keys [imported closed?] :as db}]
    (if-not closed?
      (or imported ex/examples))))



; UI derivative subs

(rf/reg-sub
  :subs.ui/output-side-tab
  :<- [:subs.query/result]
  :<- [:subs.db.ui/output-side-tab]
  (fn [[q-res out-tab]]
    (cond
      out-tab out-tab
      q-res   :db.ui.output-tab/tree
      :else   :db.ui.output-tab/attr-stats)))

(rf/reg-sub
  :subs.query/error-improved
  :<- [:subs.query/error]
  (fn [err-event]
    (when err-event
      (let [fetch-err (:evt/error err-event)
            status    (:status fetch-err)
            body-raw  (:body fetch-err)
            body-edn  (reader/read-string body-raw)]

        {:query-type (:evt/query-type err-event)
         :err/http-status  status
         :err/type
         (if (<= 500 status)
           :err.type/server
           :err.type/client)
         :err/exception body-edn}))))

(rf/reg-sub
  :subs.ui/output-main-tab
  :<- [:subs.db.ui/output-main-tab]
  :<- [:subs.query/analysis-committed]
  :<- [:subs.query/result]
  :<- [:subs.query/error]
  (fn [[out-tab q-info q-res q-err :as args]]
    (cond
      q-err :db.ui.output-tab/error
      (not q-res) :db.ui.output-tab/empty
      (= 0 (count q-res)) :db.ui.output-tab/empty
      out-tab out-tab
      (= :crux.ui.query-type/tx (:crux.ui/query-type q-info)) :db.ui.output-tab/edn
      (= 1 (count q-res)) :db.ui.output-tab/tree
      :else :db.ui.output-tab/table)))

