(ns crux.ui.events
  (:require
   [ajax.edn :as ajax-edn]
   [cljs.reader :as reader]
   [clojure.string :as string]
   [crux.ui.common :as common]
   [crux.ui.http]
   [re-frame.core :as rf]
   [tick.alpha.api :as t]))

(rf/reg-fx
 :scroll-top
 common/scroll-top)

(rf/reg-event-fx
 ::inject-metadata
 (fn [{:keys [db]} [_ title handler]]
   (let [result-meta (some-> (js/document.querySelector
                              (str "meta[title=" title "]"))
                             (.getAttribute "content"))
         edn-content (reader/read-string result-meta)]
     (if edn-content
       {:db (assoc db handler edn-content)}
       (js/console.warn "Metadata not found")))))

(rf/reg-event-db
 ::navigate-to-root-view
 (fn [db [_ view]]
   (-> (assoc-in db [:left-pane :visible?] true)
       (assoc-in [:left-pane :view] view))))

(rf/reg-event-db
 ::toggle-left-pane
 (fn [db _]
   (update-in db [:left-pane :visible?] not)))

(rf/reg-event-db
 ::set-left-pane-view
 (fn [db [_ view]]
   (assoc-in db [:left-pane :view] view)))

(rf/reg-event-db
 ::query-table-error
 (fn [db [_ error]]
   (assoc-in db [:query :error] error)))

(rf/reg-event-db
 ::set-query-right-pane-view
 (fn [db [_ view]]
   (assoc-in db [:query :right-pane :view] view)))

(rf/reg-event-db
 ::set-query-right-pane-loading
 (fn [db [_ bool]]
   (assoc-in db [:query :right-pane :loading?] bool)))

(rf/reg-event-fx
 ::go-to-query-view
 (fn [{:keys [db]} [_ {:keys [values]}]]
   (let [{:strs [q vtd vtt ttd ttt]} values
         query-params (->>
                       (merge
                        (common/edn->query-params (reader/read-string q))
                        {:valid-time (common/date-time->datetime vtd vtt)
                         :transaction-time (common/date-time->datetime ttd ttt)})
                       (remove #(nil? (second %)))
                       (into {}))]
     {:dispatch [:navigate :query {} query-params]})))

(rf/reg-event-fx
 ::set-entity-right-pane-document
 (fn [{:keys [db]} _]
   (let [query-params (-> (get-in db [:current-route :query-params])
                          (select-keys [:valid-time :transaction-time :eid]))]
     {:dispatch [:navigate :entity nil query-params]})))

(rf/reg-event-fx
 ::set-entity-right-pane-history
 (fn [{:keys [db]} _]
   (let [query-params (-> (get-in db [:current-route :query-params])
                          (assoc :history true)
                          (assoc :with-docs true)
                          (assoc :sort-order "desc"))]
     {:dispatch [:navigate :entity nil query-params]})))


(rf/reg-event-db
 ::set-entity-right-pane-loading
 (fn [db [_ bool]]
   (assoc-in db [:entity :right-pane :loading?] bool)))

(rf/reg-event-fx
 ::go-to-entity-view
 (fn [{:keys [db]} [_ {{:strs [eid vtd vtt ttd ttt]} :values}]]
   (let [query-params (->>
                       {:valid-time (common/date-time->datetime vtd vtt)
                        :transaction-time (common/date-time->datetime ttd ttt)
                        :eid eid}
                       (remove #(nil? (second %)))
                       (into {}))]
     {:db db
      :dispatch [:navigate :entity nil query-params]})))

(rf/reg-event-db
 ::entity-right-pane-document-error
 (fn [db [_ error]]
   (assoc-in db [:entity :error] error)))

(rf/reg-event-db
 ::set-entity-right-pane-history-diffs?
 (fn [db [_ bool]]
   (assoc-in db [:entity :right-pane :diffs?] bool)))
