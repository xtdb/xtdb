(ns juxt.crux-ui.frontend.events.facade
  "This ns keeps all events.
  Events for central state can be considered as FSM transitions.
  Hence ideally all state computations should be moved out of ns, but all state
  transitions (read `(rf/dispatch [:wew])`) should be kept in here."
  (:require [re-frame.core :as rf]
            [cljs.tools.reader.edn :as edn]
            [medley.core :as m]
            [juxt.crux-ui.frontend.logic.query-analysis :as qa]
            [juxt.crux-ui.frontend.logic.example-queries :as ex]
            [juxt.crux-ui.frontend.events.effects] ; just to link it
            [juxt.crux-ui.frontend.logic.history-perversions :as hp]
            [juxt.crux-ui.frontend.better-printer :as bp]
            [juxt.crux-ui.frontend.logging :as log]))


(def ^:const ui--history-max-entities 7)
(def ^:const history-tabs-set #{:db.ui.output-tab/attr-history :db.ui.output-tab/tx-history})

(defn calc-query [db ex-title]
  (if-let [imported (:db.ui.examples/imported db)]
    (:query (m/find-first #(= (:title %) ex-title) imported))
    (ex/generate ex-title)))

(defn- node-disconnected? [db]
  (not (:db.sys.host/status db)))

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

(defn query-invalid? [query-string]
  (let [edn (qa/try-parse-edn-string query-string)]
    (or (:error edn)
        (not (:crux.ui/query-type (qa/analyse-any-query edn))))))

(defn o-commit-input [db input]
  (let [input    (:db.query/input db)
        edn      (qa/try-parse-edn-string input)
        analysis (and (not (:error edn)) (qa/analyse-any-query edn))]
    (-> db
        (update :db.query/key inc)
        (assoc :db.query/input-committed input
               :db.query/analysis-committed analysis
               :db.query/edn-committed edn))))

(defn- o-db-set-host [db new-host]
  (assoc db
    :db.sys/host new-host
    :db.sys.host/status nil))

(defn- safeguard-query [analysis-committed time limit]
  (let [normalized-query (:query/normalized-edn analysis-committed)
        safeguarded-query
        (if (:limit normalized-query)
          normalized-query
          (assoc normalized-query :limit limit))]
    {:raw-input      safeguarded-query
     :query-vt       (:time/vt time)
     :query-tt       (:time/tt time)
     :query-analysis analysis-committed}))

(defn calc-query-params
  [{:db.query/keys
    [edn-committed
     analysis-committed
     time limit]
    :as db}]
  (when analysis-committed
    (case (:crux.ui/query-type analysis-committed)
      :crux.ui.query-type/query (safeguard-query analysis-committed time limit)
      :crux.ui.query-type/tx    {:raw-input edn-committed :query-analysis analysis-committed})))

(defn- pr-str' [s]
  (if (string? s) s (pr-str s)))

(defn- ^js/String calc-attr-query [{:data/keys [value type attr idx] :as evt}]
  (str "{:find [e]\n"
       " :where [[e " (pr-str' attr) (if (contains? evt :data/value) (str " " (pr-str' value))) "]]\n"
       " :full-results? true}"))

(defn get-output-tab [db]
  (get-in db [:db.sys/route :r/route-params :r/output-tab] :db.ui.output-tab/table))



; ----- composable db and fx operations -----

(defn o-set-nw-in-progress [db]
  (assoc db :db.query/network-in-progress? true))

(defn o-set-query [db ^js/String query]
  (-> db
      (assoc :db.query/input query)
      (update :db.ui/editor-key inc)))

(defn- o-fx-autoload-history [{:keys [db] :as fx}]
  (if (or (not (history-tabs-set (get-output-tab db)))
          (and (:db.query/histories db)
               (:db.query/eid->simple-history db)))
    fx
    (let [history-query
          (take ui--history-max-entities
                (:ra/entity-ids (:db.query/result-analysis db)))]
      (-> fx
          (assoc :fx.query/history history-query)
          (update :db o-set-nw-in-progress)))))

(defn o-fx-disable-polling [{db :db :as fx} ^map query-edn]
  {:db       (o-set-query db (bp/better-printer (dissoc query-edn :ui/poll-interval-seconds?)))
   :dispatch [:evt.ui.query/submit {:evt/push-url? true}]})

(defn o-fx-enable-polling [{db :db :as fx} ^map query-map]
  {:db       (o-set-query db (bp/better-printer (assoc query-map :ui/poll-interval-seconds? 5)))
   :dispatch [:evt.ui.query/submit {:evt/push-url? true}]})


(defn- o-fx-autoload-history-docs [{:keys [db] :as fx}]
  (let [histories (:db.query/histories db)
        res-an    (:db.query/result-analysis db)]
    (if-not (and histories
                 (:ra/has-numeric-attrs? res-an)
                 (history-tabs-set (get-output-tab db)))
      fx
      (assoc fx :fx.query/histories-docs histories))))



; ----- events -----

; node and lifecycle
(rf/reg-event-fx
  :evt.db/init
  (fn [{:keys [db]} [_ new-db]]
    {:db          new-db
     :fx/set-node (:db.sys/host new-db)}))

(rf/reg-event-fx
  :evt.sys/set-route
  (fn [{:keys [db]} [_ {:r/keys [query-params] :as route}]]
    (log/log :set-route route)
    (let [query (:rd/query query-params)
          host-status (:db.sys.host/status db)]
      (cond->
        {:db (cond-> (assoc db :db.sys/route route)
               query (o-set-query query))}
        (and host-status query)
        (assoc :dispatch [:evt.ui.query/submit {:evt/push-url? false}])
        ;
        true (o-fx-autoload-history)))))

(rf/reg-event-fx
  :evt.sys/node-connect-success
  (fn [{db :db :as cofx} [_ status]]
    (let [query (get-in db [:db.sys/route :r/query-params :rd/query])
          initialized? (:db.sys/initialized? db)
          do-query? (and (not initialized?) query)]
      (cond->
        {:db (assoc db :db.sys.host/status status
                       :db.sys/initialized? true)}
        do-query?
        (assoc :dispatch [:evt.ui.query/submit {:evt/push-url? false}])))))


; queries



; --- io ---

(rf/reg-event-db
  :evt.io/stats-success
  (fn [db [_ stats]]
    (assoc db :db.meta/stats stats)))


(rf/reg-event-fx
  :evt.io/gist-err
  (fn [cofx [_ res]]
    {:fx.ui/alert "Gist import didn't go well"}))

(rf/reg-event-fx
  :evt.io/query-success
  (fn [{db :db :as cofx} [_ res]]
    (let [q-info (:db.query/analysis-committed db)
          res-analysis (qa/analyse-results q-info res)
          db     (assoc db :db.query/result res
                           :db.query/result-analysis res-analysis
                           :db.query/network-in-progress? false
                           :db.query/error nil)]
      (o-fx-autoload-history {:db db}))))

(rf/reg-event-db
  :evt.io/query-error
  (fn [db [_ {:evt/keys [query-type error] :as evt}]]
    (assoc db :db.query/error evt
              :db.query/network-in-progress? false)))

(rf/reg-event-db
  :evt.db/prop-change
  (fn [db [_ {:evt/keys [prop-name value] :as evt}]]
    (assoc db prop-name value)))

(rf/reg-event-fx
  :evt.db/host-change
  (fn [{:keys [db] :as cofx} [_ new-host]]
    (println :evt.db/host-change new-host)
    (let [db (o-db-set-host db new-host)]
      {:db db
       :fx/set-node new-host})))

(rf/reg-event-fx
  :evt.io/gist-success
  (fn [{:keys [db] :as cofx} [_ res]]
    (if-let [edn (try (edn/read-string res) (catch js/Object e nil))]
      (let [db (-> (assoc db :db.ui.examples/imported edn)
                   (update o-set-example (some-> edn first :query bp/better-printer)))]
        {:db db})
      {:fx.ui/alert "Failed to parse imported gist. Is it a good EDN?"})))

(rf/reg-event-fx
  :evt.io/histories-fetch-success
  (fn [{db :db :as cofx} [_ eid->history-range]]
    (o-fx-autoload-history-docs
      {:db (assoc db
             :db.query/network-in-progress? false
             :db.query/histories eid->history-range)})))

(rf/reg-event-fx
  :evt.io/histories-with-docs-fetch-success
  (fn [{db :db :as cofx} [_ eid->history-range]]
    (let [ra (:db.query/result-analysis db)
          ts (hp/calc-entity-time-series (:ra/numeric-attrs ra) eid->history-range)]
      {:db (assoc db :db.query/histories eid->history-range
                     :db.query/network-in-progress? false
                     :db.query/eid->simple-history ts)})))

(rf/reg-event-db
  :evt.io/tx-success
  (fn [db [_ res]]
    (let [q-info (:db.query/analysis-committed db)]
      (assoc db
        :db.query/result (if (:full-results? q-info) (flatten res) res)
        :db.query/network-in-progress? false))))



; --- keyboard shortcuts ---

(rf/reg-event-fx
  :evt.keyboard/ctrl-enter
  (fn []
    {:dispatch [:evt.ui.query/submit {:evt/push-url? true}]}))



; --- ui ---



(rf/reg-event-db
  :evt.ui/show-settings
  (fn [db [_ new-size]]
    (assoc db :db.ui/screen-size new-size)))

(rf/reg-event-db
  :evt.ui/show-overview
  (fn [db [_ new-size]]
    (assoc db :db.ui/screen-size new-size)))

(rf/reg-event-fx
  :evt.ui/toggle-polling
  (fn [{:keys [db] :as cofx}]
    (let [q (qa/try-parse-edn-string-or-nil (:db.query/input db))
          analysis (some-> q qa/analyse-any-query)
          new-db (assoc db :db.ui/sidebar false)]
      (when (and q analysis (= :crux.ui.query-type/query (:crux.ui/query-type analysis)))
        (if (:ui/poll-interval-seconds? q)
          (o-fx-disable-polling {:db new-db} q)
          (o-fx-enable-polling {:db new-db} q))))))


(rf/reg-event-fx
  :evt.ui/fullscreen
  (fn [cofx]
    {:fx.ui/toggle-fullscreen true}))

(rf/reg-event-fx
  :evt.ui/restore-examples
  (fn [{:keys [db] :as cofx} [_ new-size]]
    {:db (assoc db :db.ui.examples/closed? false
                   :db.ui/sidebar false)
     :fx.sys/set-cookie [:db.ui.examples/closed? false]}))

(rf/reg-event-fx
  :evt.ui/import-examples
  (fn [{:keys [db] :as cofx} [_ new-size]]
    #_todo))



(rf/reg-event-db
  :evt.ui/screen-resize
  (fn [db [_ new-size]]
    (assoc db :db.ui/screen-size new-size)))

(rf/reg-event-db
  :evt.ui.sidebar/toggle
  (fn [db [_ new-size]]
    (update db :db.ui/sidebar not)))

(rf/reg-event-db
  :evt.ui.display-mode/toggle
  (fn [db [_ new-size]]
    (update db :db.ui/display-mode
            {:ui.display-mode/output :ui.display-mode/query
             :ui.display-mode/query :ui.display-mode/output
             :ui.display-mode/all :ui.display-mode/all})))

(defn o-cofx-query-submit [{:keys [db] :as cofx} push-url?]
  (cond
    (node-disconnected? db)
    {:fx.ui/alert "Cannot execute query until connected to a node"}
    ;
    (query-invalid? (:db.query/input db))
    {:fx.ui/alert "Query appears to be invalid"}
    ;
    :else
    (let [new-db
          (-> db
              (o-reset-results)
              (assoc :db.query/network-in-progress? true)
              (assoc :db.ui/display-mode :ui.display-mode/output)
              (o-commit-input (:db.query/input db)))
          analysis (:db.query/analysis-committed new-db)
          poll-interval (:ui/poll-interval-seconds? analysis)]
      {:db new-db
       :fx/set-polling poll-interval
       :fx/push-query-into-url (if push-url? (:db.query/input db))
       :fx/query-exec (calc-query-params new-db)})))

(rf/reg-event-fx
  :evt.ui.query/poll-tick
  (fn [{db :db :as cofx}]
    {:db (assoc db :db.query/network-in-progress? true)
     :fx/query-exec (calc-query-params db)}))

(rf/reg-event-fx
  :evt.ui.query/submit
  (fn [{:keys [db] :as cofx} [_ {:evt/keys [push-url?] :as args}]]
    (o-cofx-query-submit cofx push-url?)))

(rf/reg-event-fx
  :evt.ui.query/submit--cell
  (fn [{:keys [db] :as cofx} [_ {:data/keys [value type attr idx] :as evt}]]
    (let [query-str (calc-attr-query evt)
          new-db (-> (o-set-query db query-str)
                     (assoc-in [:db.sys/route :r/route-params :r/output-tab] :db.ui.output-tab/table))]
      (o-cofx-query-submit {:db new-db} true))))

(rf/reg-event-fx
  :evt.ui/github-examples-request
  (fn [{:keys [db] :as cofx} [_ link]]
    {:db db
     :fx/get-github-gist link}))

(rf/reg-event-fx
  :evt.ui/root-tab-switch
  (fn [{:keys [db] :as cofx} [_ root-tab-id]]
    {:db (assoc db :db.ui/root-tab root-tab-id)}))

(rf/reg-event-db
  :evt.ui.editor/set-example
  (fn [db [_ ex-title]]
    (let [query (calc-query db ex-title)
          str   (bp/better-printer query)]
      (o-set-example db str))))

(rf/reg-event-db
  :evt.ui.attr-history/disable-hint
  (fn [db]
    (assoc db :db.ui.attr-history/hint? false)))

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
  (fn [{:keys [db] :as cofx} [_ time-type time]]
    {:dispatch [:evt.ui.query/submit]}))

(rf/reg-event-db
  :evt.ui.query/time-reset
  (fn [db [_ time-type]]
    (update db :db.query/time dissoc time-type)))

(rf/reg-event-fx
  :evt.ui.output/main-tab-switch
  (fn [{:keys [db] :as cofx} [_ new-tab-id]]
    (o-fx-autoload-history {:db (assoc-in db [:db.sys/route :r/route-params :r/output-tab] new-tab-id)})))

(rf/reg-event-db
  :evt.ui.output/side-tab-switch
  (fn [db [_ new-tab-id]]
    (assoc db :db.ui/output-side-tab new-tab-id)))

(rf/reg-event-db
  :evt.ui/query-change
  (fn [db [_ query-text]]
    (assoc db :db.query/input query-text)))
