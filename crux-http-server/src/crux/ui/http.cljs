(ns crux.ui.http
  (:require
   [ajax.edn :as ajax-edn]
   [clojure.string :as string]
   [day8.re-frame.http-fx]
   [re-frame.core :as rf]
   [tick.alpha.api :as t]))


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
   (let [entity-id (-> js/window.location.pathname
                       (string/split #"/")
                       last)
         query-params (js/URLSearchParams. js/window.location.search)]
     (.set query-params "link-entities?" true)
     {:db (assoc db :entity-loading? true)
      :http-xhrio {:method :get
                   :uri (str "/_entity/" entity-id "?" query-params)
                   :response-format (ajax-edn/edn-response-format)
                   :on-success [::success-fetch-entity]
                   :on-failure [::fail-fetch-entity]}})))

(rf/reg-event-fx
 ::success-fetch-entity
 (fn [{:keys [db]} [_ result]]
   (prn "fetch entity success!")
   {:db (-> db
            (assoc :entity-data result)
            (dissoc :entity-loading?))}))

(rf/reg-event-db
 ::fail-fetch-entity
 (fn [db [_ {:keys [message] :as result}]]
   (prn "Failure: get fetch entity result: " result)
   (dissoc db :entity-loading?)))
