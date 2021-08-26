(ns xtdb.ui.http
  (:require
   [ajax.edn :as ajax-edn]
   [ajax.protocols :refer [-body]]
   [cljs.reader :as reader]
   [ajax.interceptors :refer [map->ResponseFormat]]
   [xtdb.ui.common :as common]
   [day8.re-frame.http-fx]
   [re-frame.core :as rf]
   [tick.alpha.api :as t]
   [xtdb.http-server.entity-ref :as entity-ref]))

(def edn-response-with-readers
  (map->ResponseFormat {:read (fn [xhrio]
                                (reader/read-string
                                 {:readers {'xt.http/entity-ref entity-ref/->EntityRef
                                            'xt/id str}}
                                 (-body xhrio)))
                        :description "EDN"
                        :content-type ["application/edn"]}))

(rf/reg-event-fx
 ::fetch-query-table
 (fn [{:keys [db]} _]
   (let [meta-results (get-in db [:meta-results :query-results])]
     (cond
       meta-results {:db (-> (if-let [error (get meta-results "error")]
                               (assoc-in db [:query :error] (str error))
                               (assoc-in db [:query :http] meta-results))
                             (assoc :meta-results nil))
                     :dispatch [:xtdb.ui.events/set-query-result-pane-loading false]}
       (:load-from-state? db) {:db (dissoc db :load-from-state?)
                               :dispatch [:xtdb.ui.events/set-query-result-pane-loading false]}
       :else (let [{:keys [query] :as query-params} (get-in db [:current-route :query-params])
                   now (t/now)
                   ;; Potential smell here - query params get turned to strings, then read back out here
                   ;; (to edit contents of query with parameters outside of the displayed URL).
                   parsed-query (reader/read-string query)
                   ;; Get back one more result than necessary - won't be rendered,
                   ;; but used to check if there are more results in the table
                   limit (+ 1 (:limit parsed-query 100))]
               (when (seq query-params)
                 {:scroll-top nil
                  :db (-> (assoc-in db [:request :start-time] now)
                          (assoc-in [:request :end-time] now))
                  :dispatch-n [[:xtdb.ui.events/set-query-result-pane-loading true]
                               [:xtdb.ui.events/query-table-error nil]]
                  :http-xhrio {:method :get
                               :uri (common/route->url :query {} (merge
                                                                  query-params
                                                                  {:query (assoc parsed-query :limit limit)
                                                                   :link-entities? true}))
                               :response-format edn-response-with-readers
                               :on-success [::success-fetch-query-table]
                               :on-failure [::fail-fetch-query-table]}}))))))

(rf/reg-event-fx
 ::success-fetch-query-table
 (fn [{:keys [db]} [_ result]]
   (prn "fetch query table success!")
   {:dispatch-n [[:xtdb.ui.events/set-query-result-pane-loading false]
                 [:xtdb.ui.events/toggle-form-pane true]]
    :db (->
         (assoc-in db [:query :http] result)
         (assoc-in [:request :end-time] (t/now)))}))

(rf/reg-event-fx
 ::fail-fetch-query-table
 (fn [{:keys [db]} [_ result]]
   (prn "Failure: get query table result: " result)
   {:dispatch [:xtdb.ui.events/set-query-result-pane-loading false]
    :db (assoc-in db [:query :error] (:response result))}))

(rf/reg-event-fx
 ::fetch-node-status
 (fn [{:keys [db]} _]
   (let [meta-results (get-in db [:meta-results :status-results])]
     (cond
       meta-results {:db (-> (assoc-in db [:status :http] (:status-map meta-results))
                             (assoc-in [:attribute-stats :http] (:attribute-stats meta-results))
                             (assoc :meta-results nil))
                     :dispatch [:xtdb.ui.events/set-node-status-loading false]}
       :else {:scroll-top nil
              :db (dissoc db :load-from-state?)
              :dispatch [:xtdb.ui.events/set-node-status-loading true]
              :http-xhrio {:method :get
                           :uri (common/route->url :status)
                           :response-format (ajax-edn/edn-response-format)
                           :on-success [::success-fetch-node-status]
                           :on-failure [::fail-fetch-node-status]}}))))

(rf/reg-event-fx
 ::success-fetch-node-status
 (fn [{:keys [db]} [_ result]]
   (prn "fetch node status success!")
   {:dispatch [:xtdb.ui.events/set-node-status-loading false]
    :db (assoc-in db [:status :http] result)}))

(rf/reg-event-fx
 ::fail-fetch-node-status
 (fn [{:keys [db]} [_ result]]
   (prn "Failure: get node status: " result)
   {:dispatch [:xtdb.ui.events/set-node-status-loading false]
    :db (assoc-in db [:status :error] (:response result))}))

(rf/reg-event-fx
 ::fetch-node-attribute-stats
 (fn [{:keys [db]} _]
   {:scroll-top nil
    :dispatch [:xtdb.ui.events/set-node-attribute-stats-loading true]
    :http-xhrio {:method :get
                 :uri (common/route->url :attribute-stats)
                 :response-format (ajax-edn/edn-response-format)
                 :on-success [::success-fetch-node-attribute-stats]
                 :on-failure [::fail-fetch-node-attribute-stats]}}))

(rf/reg-event-fx
 ::success-fetch-node-attribute-stats
 (fn [{:keys [db]} [_ result]]
   (prn "fetch node attribute-stats success!")
   {:dispatch [:xtdb.ui.events/set-node-attribute-stats-loading false]
    :db (assoc-in db [:attribute-stats :http] result)}))

(rf/reg-event-fx
 ::fail-fetch-node-attribute-stats
 (fn [{:keys [db]} [_ result]]
   (prn "Failure: get node attribute status: " result)
   {:dispatch [:xtdb.ui.events/set-node-status-loading false]
    :db (assoc-in db [:status :error] (:response result))}))

(rf/reg-event-fx
 ::fetch-entity
 (fn [{:keys [db]} _]
   (let [meta-results (get-in db [:meta-results :entity-results])
         query-params (get-in db [:current-route :query-params])]
     (cond
       meta-results (let [result-pane-view (if (:history query-params) :history :document)]
                      {:db (-> (if-let [error (get meta-results "error")]
                                 (assoc-in db [:entity :error] (str error))
                                 (assoc-in db [:entity :http result-pane-view] meta-results))
                               (assoc :meta-results nil))
                       :dispatch [:xtdb.ui.events/set-entity-result-pane-loading false]})
       (:load-from-state? db) {:db (dissoc db :load-from-state?)
                               :dispatch [:xtdb.ui.events/set-entity-result-pane-loading false]}
       :else (when (seq query-params)
               {:scroll-top nil
                :dispatch-n [[:xtdb.ui.events/entity-result-pane-document-error nil]
                             [:xtdb.ui.events/set-entity-result-pane-loading true]]
                :http-xhrio {:method :get
                             :uri (common/route->url :entity nil (assoc query-params :link-entities? true))
                             :response-format edn-response-with-readers
                             :on-success [::success-fetch-entity]
                             :on-failure [::fail-fetch-entity]}})))))

(rf/reg-event-fx
 ::success-fetch-entity
 (fn [{:keys [db]} [_ result]]
   (prn "fetch entity success!")
   (let [result-pane-view (if (get-in db [:current-route :query-params :history]) :history :document)]
     {:db (assoc-in db [:entity :http result-pane-view] result)
      :dispatch-n [[:xtdb.ui.events/set-entity-result-pane-loading false]
                   [:xtdb.ui.events/toggle-form-pane true]]})))

(rf/reg-event-fx
 ::fail-fetch-entity
 (fn [{:keys [db]} [_ result]]
   (prn "Failure: fetch entity " result)
   {:db (assoc-in db [:entity :error] (:response result))
    :dispatch [:xtdb.ui.events/set-entity-result-pane-loading false]}))
