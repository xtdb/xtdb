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
 ::toggle-left-pane
 (fn [db _]
   (update-in db [:left-pane :visible?] not)))

(rf/reg-event-db
 ::set-left-pane-view
 (fn [db [_ view]]
   (assoc-in  db [:left-pane :view] view)))

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
   (let [{:strs [q vt tt]} values
         query-params (->>
                       (merge
                        (common/edn->query-params (reader/read-string q))
                        {:valid-time (common/instant->date-time vt)
                         :transaction-time (common/instant->date-time tt)})
                       (remove #(nil? (second %)))
                       (into {}))]
     {:dispatch [:navigate :query {} query-params]})))

(rf/reg-event-db
 ::set-entity-right-pane-view
 (fn [db [_ view]]
   (assoc-in db [:entity :right-pane :view] view)))

(rf/reg-event-db
 ::set-entity-right-pane-loading
 (fn [db [_ bool]]
   (assoc-in db [:entity :right-pane :loading?] bool)))

(rf/reg-event-fx
 ::go-to-entity-view
 (fn [{:keys [db]} [_ {{:strs [vt tt eid]} :values}]]
   (let [query-params (->>
                       {:valid-time (common/instant->date-time vt)
                        :transaction-time (common/instant->date-time tt)}
                       (remove #(nil? (second %)))
                       (into {}))]
     {:db db
      :dispatch [:navigate :entity {:eid eid} query-params]})))
