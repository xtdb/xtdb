(ns crux.ui.events
  (:require
   [ajax.edn :as ajax-edn]
   [day8.re-frame.http-fx]
   [re-frame.core :as rf]))

(rf/reg-event-fx
 ::get-query-result
 (fn [{:keys [db]} _]
   {:http-xhrio {:method :get
                 :uri "/_query?find=[id]&where=[id :crux.db/id _]"
                 :response-format (ajax-edn/edn-response-format)
                 :on-success [::success-get-query-result]
                 :on-failure [::fail-get-query-result]}}))

(rf/reg-event-db
 ::success-get-query-result
 (fn [db [_ result]]
   (assoc db :query-data result)))

(rf/reg-event-db
 ::fail-get-query-result
 (fn [db [_ result]]
   (prn "Failure: get query result: " result)
   db))
