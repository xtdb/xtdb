(ns juxt.crux-ui.frontend.events.facade
  "This ns keeps all events.
  Events for central state can be considered as FSM transitions.
  Hence ideally all state computations should be moved out of ns, but all state
  transitions (read `(rf/dispatch [:wew])`) should be kept in here."
  (:require [re-frame.core :as rf]
            [cljs.tools.reader.edn :as edn]
            [medley.core :as m]
            [promesa.core :as p]
            [juxt.crux-ui.frontend.io.query :as q]
            [juxt.crux-ui.frontend.logic.query-analysis :as qa]
            [juxt.crux-ui.frontend.example-queries :as ex]
            [juxt.crux-ui.frontend.functions :as f]
            [juxt.crux-ui.frontend.cookies :as c]
            [juxt.crux-ui.frontend.logic.history-perversions :as hp]
            [juxt.crux-lib.http-functions :as hf]
            [juxt.crux-ui.frontend.better-printer :as bp]))


(defn calc-query [db ex-title]
  (if-let [imported (:db.ui.examples/imported db)]
    (:query (m/find-first #(= (:title %) ex-title) imported))
    (ex/generate ex-title)))

(defn o-set-example [db str]
  (-> db
      (update :db.ui/editor-key inc)
      (assoc :db.query/input str
             :db.query/input-committed str)))

(defn o-reset-results [db]
  (assoc db
    :db.query/result nil
    :db.query/result-analysis nil
    :db.query/histories nil
    :db.query/eid->simple-history nil
    :db.query/analysis-committed nil
    :db.query/input-committed nil
    :db.query/error nil))

(defn query-is-valid? [query-string]
  (not (:error (qa/try-read-string query-string))))

(defn o-commit-input [db input]
  (let [input    (:db.query/input db)
        edn      (qa/try-read-string input)
        analysis (and (not (:error edn)) (qa/analyse-query edn))]
    (-> db
        (update :db.query/key inc)
        (assoc :db.query/input-committed input
               :db.query/analysis-committed analysis
               :db.query/edn-committed edn))))

(defn calc-query-params
  [{:db.query/keys
        [analysis-committed time
         input-committed]
    :as db}]
  (if analysis-committed
    {:raw-input      input-committed
     :query-vt       (:time/vt time)
     :query-tt       (:time/tt time)
     :query-analysis analysis-committed}))


; ----- effects -----

(rf/reg-fx
  :fx/query-exec
  (fn [{:keys [raw-input query-analysis] :as query}]
    (if query
      (q/exec query))))

(rf/reg-fx
  :fx/query-stats
  (fn [_]
    (q/fetch-stats)))

(rf/reg-fx
  :fx.query/history
  (fn [eids]
    (q/fetch-histories eids)))

(rf/reg-fx
  :fx.query/histories-docs
  (fn [eids->histories]
    (q/fetch-histories-docs eids->histories)))

(rf/reg-fx
  :fx.sys/set-cookie
  (fn [[cookie-name value]]
    (c/set! cookie-name value {:max-age 86400})))

(rf/reg-fx
  :fx.ui/alert
  (fn [message]
    (js/alert message)))

(defn grab-gh-gist [gh-link]
  (-> (hf/fetch gh-link)
      (p/catch #(rf/dispatch [:evt.io/gist-err %]))
      (p/then #(rf/dispatch [:evt.io/gist-success (:body %)]))))

(rf/reg-fx
  :fx/get-github-gist
  grab-gh-gist)

(def url "https://gist.githubusercontent.com/spacegangster/b68f72e3c81524a71af1f3033ea7507e/raw/572396dec0791500c965fea443b2f26a60f500d4/examples.edn")



; ----- events -----

; node and lifecycle
(rf/reg-event-fx
  :evt.db/init
  (fn [_ [_ db]]
    {:db             db
     :fx/query-stats nil}))


; queries

(rf/reg-event-db
  :evt.io/stats-success
  (fn [db [_ stats]]
    (assoc db :db.meta/stats stats)))



; --- io ---

(def ^:const ui--history-max-entities 7)
(def ^:const history-tabs-set #{:db.ui.output-tab/attr-history :db.ui.output-tab/tx-history})

(defn- ctx-autoload-history [{:keys [db] :as new-ctx}]
  (if-not (history-tabs-set (:db.ui/output-main-tab db))
    new-ctx
    (assoc new-ctx :fx.query/history
                   (take ui--history-max-entities
                         (:ra/entity-ids (:db.query/result-analysis db))))))

(defn- ctx-autoload-history-docs [{:keys [db] :as new-ctx}]
  (let [histories (:db.query/histories db)
        res-an    (:db.query/result-analysis db)]
    (if-not (and histories
                 (:ra/has-numeric-attrs? res-an)
                 (history-tabs-set (:db.ui/output-main-tab db)))
      new-ctx
      (assoc new-ctx :fx.query/histories-docs histories))))


(rf/reg-event-fx
  :evt.io/query-success
  (fn [{db :db :as ctx} [_ res]]
    (let [q-info (:db.query/analysis-committed db)
          res-analysis (qa/analyse-results q-info res)
          db     (assoc db :db.query/result res
                           :db.query/error nil
                           :db.query/result-analysis res-analysis)]

      (ctx-autoload-history {:db db}))))

(rf/reg-event-fx
  :evt.io/gist-err
  (fn [ctx [_ res]]
    (assoc ctx :fx.ui/alert "Gist import didn't go well")))

(rf/reg-event-db
  :evt.io/query-error
  (fn [db [_ {:evt/keys [query-type error] :as evt}]]
    (assoc db :db.query/error evt)))

(rf/reg-event-fx
  :evt.io/gist-success
  (fn [{:keys [db] :as ctx} [_ res]]
    (if-let [edn (try (edn/read-string res) (catch js/Object e nil))]
      (-> ctx
          (assoc-in [:db :db.ui.examples/imported] edn)
          (update :db o-set-example (some-> edn first :query bp/better-printer)))
      (assoc ctx :fx.ui/alert "Failed to parse imported gist. Is it a good EDN?"))))

(rf/reg-event-fx
  :evt.io/histories-fetch-success
  (fn [{db :db :as ctx} [_ eid->history-range]]
    (ctx-autoload-history-docs {:db (assoc db :db.query/histories eid->history-range)})))

(rf/reg-event-fx
  :evt.io/histories-with-docs-fetch-success
  (fn [{db :db :as ctx} [_ eid->history-range]]
    (let [ra (:db.query/result-analysis db)
          ts (hp/calc-entity-time-series (:ra/numeric-attrs ra) eid->history-range)]
      {:db (assoc db :db.query/histories eid->history-range
                     :db.query/eid->simple-history ts)})))

(rf/reg-event-db
  :evt.io/tx-success
  (fn [db [_ res]]
    (let [q-info (:db.query/analysis-committed db)]
      (assoc db :db.query/result
                (if (:full-results? q-info)
                  (flatten res) res)))))



; --- keyboard shortcuts ---

(rf/reg-event-fx
  :evt.keyboard/ctrl-enter
  (fn []
    {:dispatch [:evt.ui/query-submit]}))



; --- ui ---

(rf/reg-event-fx
  :evt.ui/query-submit
  (fn [{:keys [db] :as ctx}]
    (if (query-is-valid? (:db.query/input db))
      (let [new-db
            (-> db
                (o-reset-results)
                (o-commit-input (:db.query/input db)))]
        {:db new-db
         :fx/query-exec (calc-query-params new-db)}))))

(rf/reg-event-fx
  :evt.ui/github-examples-request
  (fn [{:keys [db] :as ctx} [_ link]]
    {:db db
     :fx/get-github-gist link}))

(rf/reg-event-fx
  :evt.ui/root-tab-switch
  (fn [{:keys [db] :as ctx} [_ root-tab-id]]
    {:db (assoc db :db.ui/root-tab root-tab-id)}))

(rf/reg-event-db
  :evt.ui.editor/set-example
  (fn [db [_ ex-title]]
    (let [query (calc-query db ex-title)
          str   (bp/better-printer query)]
      (o-set-example db str))))

(rf/reg-event-fx
  :evt.ui.examples/close
  (fn [{db :db}]
    {:db (assoc db :db.ui.examples/closed? true)
     :fx.sys/set-cookie [:db.ui.examples/closed? true]}))

(rf/reg-event-db
  :evt.ui.query/time-change
  (fn [db [_ time-type time]]
    (assoc-in db [:db.query/time time-type] time)))

(rf/reg-event-fx
  :evt.ui.query/time-commit
  (fn [{:keys [db] :as ctx} [_ time-type time]]
    {:dispatch [:evt.ui/query-submit]}))

(rf/reg-event-db
  :evt.ui.query/time-reset
  (fn [db [_ time-type]]
    (update db :db.query/time dissoc time-type)))

(rf/reg-event-fx
  :evt.ui.output/main-tab-switch
  (fn [{:keys [db] :as ctx} [_ new-tab-id]]
    (ctx-autoload-history {:db (assoc db :db.ui/output-main-tab new-tab-id)})))

(rf/reg-event-db
  :evt.ui.output/side-tab-switch
  (fn [db [_ new-tab-id]]
    (assoc db :db.ui/output-side-tab new-tab-id)))

(rf/reg-event-db
  :evt.ui/query-change
  (fn [db [_ query-text]]
    (assoc db :db.query/input query-text)))
