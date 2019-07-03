(ns juxt.crux-ui.frontend.events.facade
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.io.query :as q]
            [juxt.crux-ui.frontend.logic.query-analysis :as qa]
            [juxt.crux-ui.frontend.example-queries :as ex]
            [juxt.crux-ui.frontend.functions :as f]))



; ----- effects -----

(rf/reg-fx
  :fx/query-exec
  (fn [{:keys [raw-input query-analysis] :as query}]
    (q/exec query)))

(rf/reg-fx
  :fx/query-stats
  (fn [_]
    (q/fetch-stats)))



; ----- events -----

(rf/reg-event-fx
  :evt.db/init
  (fn [_ [_ db]]
    {:db             db
     :fx/query-stats nil}))

(rf/reg-event-db
  :evt.io/stats-success
  (fn [db [_ stats]]
    (assoc db :db.meta/stats stats)))

(rf/reg-event-db
  :evt.io/query-success
  (fn [db [_ res]]
    (let [q-info (:db.query/analysis-committed db)]
      (assoc db :db.query/result
                (if (:full-results? q-info)
                  (flatten res) res)))))

(rf/reg-event-db
  :evt.io/tx-success
  (fn [db [_ res]]
    (let [q-info (:db.query/analysis-committed db)]
      (assoc db :db.query/result
                (if (:full-results? q-info)
                  (flatten res) res)))))

(rf/reg-event-fx
  :evt.keyboard/ctrl-enter
  (fn []
    {:dispatch [:evt.ui/query-submit]}))

(rf/reg-event-fx
  :evt.ui/query-submit
  (fn [{:keys [db] :as ctx}]
    (let [input (:db.query/input db)
          edn (qa/try-read-string input)
          query-times (:db.query/time db)
          vt (:crux.ui.time-type/vt query-times)
          tt (:crux.ui.time-type/tt query-times)
          analysis (and (not (:error edn)) (qa/analyse-query edn))]
      {:db            (-> db
                          (update :db.query/key inc)
                          (assoc :db.query/input-committed input
                                 :db.query/analysis-committed analysis
                                 :db.query/edn-committed edn
                                 :db.query/result nil))
       :fx/query-exec {:raw-input      input
                       :query-vt vt
                       :query-tt tt
                       :query-analysis analysis}})))


(rf/reg-event-db
  :evt.ui.editor/set-example
  (fn [db [_ ex-id]]
    (let [ex-edn (ex/generate ex-id)
          str (f/pprint-str ex-edn)]
      (-> db
          (update :db.ui/editor-key inc)
          (assoc :db.query/input str
                 :db.query/input-committed str)))))


(rf/reg-event-db
  :evt.ui.query/time-change
  (fn [db [_ time-type time]]
    (assoc-in db [:db.query/time time-type] time)))

(rf/reg-event-db
  :evt.ui.query/time-reset
  (fn [db [_ time-type]]
    (update db :db.query/time dissoc time-type)))

(rf/reg-event-db
  :evt.ui.output/tab-switch
  (fn [db [_ new-tab-id]]
    (assoc db :db.ui/output-tab new-tab-id)))

(rf/reg-event-db
  :evt.ui/query-change
  (fn [db [_ query-text]]
    (assoc db :db.query/input query-text)))
