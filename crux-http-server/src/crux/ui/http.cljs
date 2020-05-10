(ns crux.ui.http
  (:require
   [ajax.edn :as ajax-edn]
   [crux.ui.common :as common]
   [day8.re-frame.http-fx]
   [re-frame.core :as rf]))

(rf/reg-event-fx
 ::fetch-query-table
 (fn [{:keys [db]} _]
   (let [query-params (doto (js/URLSearchParams. js/window.location.search) (.delete "full-results"))
         find "1"
         link-entities? (.get query-params "link-entities?")]
     (when (seq (str query-params))
       {:db (assoc db :table-loading? true)
        :http-xhrio {:method :get
                     :uri (str "/_query?" query-params (when-not link-entities?
                                                         "&link-entities?=true"))
                     :response-format (ajax-edn/edn-response-format)
                     :on-success [::success-fetch-query-table find]
                     :on-failure [::fail-fetch-query-table]}}))))

(rf/reg-event-fx
 ::success-fetch-query-table
 (fn [{:keys [db]} [_ find-clause result]]
   (prn "fetch query table success!")
   {:db (-> db
            (assoc :query-data
                   (assoc result "find-clause" find-clause))
            (assoc :table-loading? false))}))

(rf/reg-event-db
 ::fail-fetch-query-table
 (fn [db [_ result]]
   (prn "Failure: get query table result: " result)
   (-> db
       (assoc-in [:query-data :error]
                 (get-in result [:response :via 0 :message]))
       (assoc :table-loading? false))))

(rf/reg-event-fx
 ::fetch-entity
 (fn [{:keys [db]} _]
   (let [eid (get-in db [:current-route :path-params :eid])
         query-params (assoc (get-in db [:current-route :query-params])
                             :link-entities? true)]
     {:dispatch [:crux.ui.events/set-entity-right-pane-loading true]
      :http-xhrio {:method :get
                   :uri (common/route->url :entity {:eid eid} query-params)
                   :response-format (ajax-edn/edn-response-format)
                   :on-success [::success-fetch-entity]
                   :on-failure [::fail-fetch-entity]}})))

(rf/reg-event-fx
 ::success-fetch-entity
 (fn [{:keys [db]} [_ result]]
   (prn "fetch entity success!")
   {:db (assoc-in db [:entity :http] result)
    :dispatch [:crux.ui.events/set-entity-right-pane-loading false]}))

(rf/reg-event-fx
 ::fail-fetch-entity
 (fn [{:keys [db]} [_ {:keys [message] :as result}]]
   (prn "Failure: get fetch entity result: " result)
   {:dispatch [:crux.ui.events/set-entity-right-pane-loading false]}))
