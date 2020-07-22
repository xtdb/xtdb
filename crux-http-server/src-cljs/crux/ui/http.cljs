(ns crux.ui.http
  (:require
   [ajax.edn :as ajax-edn]
   [crux.ui.common :as common]
   [day8.re-frame.http-fx]
   [re-frame.core :as rf]
   [tick.alpha.api :as t]))

(rf/reg-event-fx
 ::fetch-query-table
 (fn [{:keys [db]} _]
   (let [meta-results (get-in db [:meta-results :query-results])]
     (if meta-results
       {:db (-> (if-let [error (get meta-results "error")]
                  (assoc-in db [:query :error] (str error))
                  (assoc-in db [:query :http] meta-results))
                (assoc :meta-results nil))
        :dispatch [:crux.ui.events/set-query-result-pane-loading false]}
       (let [query-params (dissoc (get-in db [:current-route :query-params]) :full-results)
             ;; Get back one more result than necessary - won't be rendered,
             ;; but used to check if there are more results in the table
             limit (+ 1 (js/parseInt (:limit query-params 100)))
             now (t/now)]
         (when (seq query-params)
           {:scroll-top nil
            :db (-> (assoc-in db [:request :start-time] now)
                    (assoc-in [:request :end-time] now))
            :dispatch-n [[:crux.ui.events/set-query-result-pane-loading true]
                         [:crux.ui.events/query-table-error nil]]
            :http-xhrio {:method :get
                         :uri (common/route->url :query
                                                 {}
                                                 (assoc query-params
                                                        :link-entities? true
                                                        :limit limit))
                         :response-format (ajax-edn/edn-response-format)
                         :on-success [::success-fetch-query-table]
                         :on-failure [::fail-fetch-query-table]}}))))))

(rf/reg-event-fx
 ::success-fetch-query-table
 (fn [{:keys [db]} [_ result]]
   (prn "fetch query table success!")
   {:dispatch-n [[:crux.ui.events/set-query-result-pane-loading false]
                 [:crux.ui.events/toggle-form-pane true]]
    :db (->
         (assoc-in db [:query :http] result)
         (assoc-in [:request :end-time] (t/now)))}))

(rf/reg-event-fx
 ::fail-fetch-query-table
 (fn [{:keys [db]} [_ result]]
   (prn "Failure: get query table result: " result)
   {:dispatch [:crux.ui.events/set-query-result-pane-loading false]
    :db (assoc-in db [:query :error] (get-in result [:response :error]))}))

(rf/reg-event-fx
 ::fetch-node-status
 (fn [{:keys [db]} _]
   {:scroll-top nil
    :dispatch [:crux.ui.events/set-node-status-loading true]
    :http-xhrio {:method :get
                 :uri (common/route->url :status)
                 :response-format (ajax-edn/edn-response-format)
                 :on-success [::success-fetch-node-status]
                 :on-failure [::fail-fetch-node-status]}}))

(rf/reg-event-fx
 ::success-fetch-node-status
 (fn [{:keys [db]} [_ result]]
   (prn "fetch node status success!")
   {:dispatch [:crux.ui.events/set-node-status-loading false]
    :db (assoc-in db [:status :http] result)}))

(rf/reg-event-fx
 ::fail-fetch-node-status
 (fn [{:keys [db]} [_ result]]
   (prn "Failure: get node status: " result)
   {:dispatch [:crux.ui.events/set-node-status-loading false]
    :db (assoc-in db [:status :error] result)}))

(rf/reg-event-fx
 ::fetch-node-attribute-stats
 (fn [{:keys [db]} _]
   {:scroll-top nil
    :dispatch [:crux.ui.events/set-node-attribute-stats-loading true]
    :http-xhrio {:method :get
                 :uri (common/route->url :attribute-stats)
                 :response-format (ajax-edn/edn-response-format)
                 :on-success [::success-fetch-node-attribute-stats]
                 :on-failure [::fail-fetch-node-attribute-stats]}}))

(rf/reg-event-fx
 ::success-fetch-node-attribute-stats
 (fn [{:keys [db]} [_ result]]
   (prn "fetch node attribute-stats success!")
   {:dispatch [:crux.ui.events/set-node-attribute-stats-loading false]
    :db (assoc-in db [:attribute-stats :http] result)}))

(rf/reg-event-fx
 ::fail-fetch-node-status
 (fn [{:keys [db]} [_ result]]
   (prn "Failure: get node status: " result)
   {:dispatch [:crux.ui.events/set-node-status-loading false]
    :db (assoc-in db [:status :error] result)}))

(rf/reg-event-fx
 ::fetch-entity
 (fn [{:keys [db]} _]
   (let [meta-results (get-in db [:meta-results :entity-results])]
     (if meta-results
       (let [result-pane-view (if (get-in db [:current-route :query-params :history]) :history :document)]
         {:db (-> (if-let [error (get meta-results "error")]
                    (assoc-in db [:entity :error] (str error))
                    (assoc-in db [:entity :http result-pane-view] meta-results))
                  (assoc :meta-results nil))
          :dispatch [:crux.ui.events/set-entity-result-pane-loading false]})
       (let [query-params (assoc (get-in db [:current-route :query-params]) :link-entities? true)]
         (when (seq (dissoc query-params :linked-entities))
           {:scroll-top nil
            :dispatch-n [[:crux.ui.events/entity-result-pane-document-error nil]
                         [:crux.ui.events/set-entity-result-pane-loading true]]
            :http-xhrio {:method :get
                         :uri (common/route->url :entity nil query-params)
                         :response-format (ajax-edn/edn-response-format)
                         :on-success [::success-fetch-entity]
                         :on-failure [::fail-fetch-entity]}}))))))

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
   (prn "Failure: fetch entity " result)
   {:db (assoc-in db [:entity :error] (get-in result [:response :error]))
    :dispatch [:crux.ui.events/set-entity-result-pane-loading false]}))
