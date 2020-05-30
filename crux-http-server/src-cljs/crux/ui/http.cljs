(ns crux.ui.http
  (:require
   [ajax.edn :as ajax-edn]
   [crux.ui.common :as common]
   [day8.re-frame.http-fx]
   [re-frame.core :as rf]))

(rf/reg-event-fx
 ::fetch-query-table
 (fn [{:keys [db]} _]
   (let [query-params (dissoc (get-in db [:current-route :query-params]) :full-results)]
     (when (seq query-params)
       {:scroll-top nil
        :dispatch-n [[:crux.ui.events/set-query-result-pane-loading true]
                     [:crux.ui.events/query-table-error nil]]
        :http-xhrio {:method :get
                     :uri (common/route->url :query
                                             {}
                                             (assoc query-params :link-entities? true))
                     :response-format (ajax-edn/edn-response-format)
                     :on-success [::success-fetch-query-table]
                     :on-failure [::fail-fetch-query-table]}}))))

(rf/reg-event-fx
 ::success-fetch-query-table
 (fn [{:keys [db]} [_ result]]
   (prn "fetch query table success!")
   {:dispatch-n [[:crux.ui.events/set-query-result-pane-loading false]
                 [:crux.ui.events/toggle-form-pane true]]
    :db (assoc-in db [:query :http] result)}))

(rf/reg-event-fx
 ::fail-fetch-query-table
 (fn [{:keys [db]} [_ result]]
   (prn "Failure: get query table result: " result)
   {:dispatch [:crux.ui.events/set-query-result-pane-loading false]
    :db (assoc-in db [:query :error]
                  (get-in result [:response :via 0 :message]))}))

(rf/reg-event-fx
 ::fetch-entity
 (fn [{:keys [db]} _]
   (let [query-params (assoc (get-in db [:current-route :query-params]) :link-entities? true)]
     {:scroll-top nil
      :dispatch-n [[:crux.ui.events/entity-result-pane-document-error nil]
                   [:crux.ui.events/set-entity-result-pane-loading true]]
      :http-xhrio {:method :get
                   :uri (common/route->url :entity nil query-params)
                   :response-format (ajax-edn/edn-response-format)
                   :on-success [::success-fetch-entity]
                   :on-failure [::fail-fetch-entity]}})))

(rf/reg-event-fx
 ::success-fetch-entity
 (fn [{:keys [db]} [_ result]]
   (prn "fetch entity success!")
   (let [result-pane-view (if (get-in db [:current-route :query-params :history]) :history :document)]
     {:db (assoc-in db [:entity :http result-pane-view] result)
      :dispatch-n [[:crux.ui.events/set-entity-result-pane-loading false]
                   [:crux.ui.events/toggle-form-pane true]]})))

(rf/reg-event-fx
 ::fail-fetch-entity
 (fn [{:keys [db]} [_ result]]
   {:db (assoc-in db [:entity :error] (str result))
    :dispatch [:crux.ui.events/set-entity-result-pane-loading false]}))
