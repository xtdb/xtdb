(ns crux.ui.events
  (:require
   [ajax.edn :as ajax-edn]
   [cljs.reader :refer [read-string]]
   [day8.re-frame.http-fx]
   [re-frame.core :as rf]))

(rf/reg-event-fx
 ::inject-metadata
 (fn [{:keys [db]} [_ title handler]]
   (let [result-meta (some-> (js/document.querySelector
                              (str "meta[title=" title "]"))
                             (.getAttribute "content"))
         edn-content (read-string result-meta)]
     (if edn-content
       {:db (assoc db handler edn-content)}
       {:db db}))))

(defn cast-to-query-params
  [query]
  (let [{:keys [find where limit
                offset args order-by]} query]
    (cond-> ""
      :find (str "find=" find)
      :where ((fn [x] (apply
                       str x
                       (map #(str "&where=" %) where))))
      limit (str "&limit=" limit)
      offset (str "&offset=" offset)
      args (str "&args=" args)
      order-by ((fn [x]
                  (apply
                   str x
                   (map #(str "&order-by" %) order-by))))
      true (str "&link-entities?=true"))))

(rf/reg-event-fx
 ::go-to-query-table
 (fn [{:keys [db]} [_ query-value]]
   (let [query-map (read-string query-value)
         query-params (cast-to-query-params query-map)]
     {:dispatch [:navigate {:page :query
                            :query-params query-params}]})))

(rf/reg-event-fx
 ::fetch-query-table
 (fn [{:keys [db]} [_ & [query-params]]]
   (let [query-params (or query-params (get-in db [:current-page :query-params]))
         find (read-string (.get (js/URLSearchParams. js/window.location.search) "find"))
         link-entities? (.get (js/URLSearchParams. query-params) "link-entities")]
     {:http-xhrio {:method :get
                   :uri (str "/_query?" query-params (when-not link-entities?
                                                       "&link-entities?=true"))
                   :response-format (ajax-edn/edn-response-format)
                   :on-success [::success-fetch-query-table query-params find]
                   :on-failure [::fail-fetch-query-table]}})))

(rf/reg-event-fx
 ::success-fetch-query-table
 (fn [{:keys [db]} [_ query-params find-clause result]]
   (prn "fetch query table success!")
   {:db (assoc db :query-data
               (assoc result "find-clause" find-clause))
    :dispatch [:navigate {:page :query
                          :query-params query-params}]}))

(rf/reg-event-db
 ::fail-fetch-query-table
 (fn [db [_ result]]
   (prn "Failure: get query table result: " result)
   db))

(rf/reg-event-fx
 ::fetch-entity
 (fn [{:keys [db]} _]
   (let [entity-id (get-in db [:current-page :route-params :entity-id])
         query-params (get-in db [:current-page :query-params])
         link-entities? (.get (js/URLSearchParams. query-params) "link-entities")]
     {:http-xhrio {:method :get
                   :uri (str "/_entity/" entity-id "?" query-params
                             (when-not link-entities? "&link-entities?=true"))
                   :response-format (ajax-edn/edn-response-format)
                   :on-success [::success-fetch-entity entity-id query-params]
                   :on-failure [::fail-fetch-entity]}})))

(rf/reg-event-fx
 ::success-fetch-entity
 (fn [{:keys [db]} [_ entity-id query-params result]]
   (prn "fetch entity success!")
   {:db (assoc db :entity-data result)
    :dispatch [:navigate {:page :entity
                          :path-params {:entity-id entity-id}
                          :query-params query-params}]}))

(rf/reg-event-db
 ::fail-fetch-entity
 (fn [db [_ result]]
   (prn "Failure: get query table result: " result)
   db))
