(ns crux.ui.events
  (:require
   [ajax.edn :as ajax-edn]
   [cljs.reader :refer [read-string]]
   [day8.re-frame.http-fx]
   [re-frame.core :as rf]))

(rf/reg-event-db
 ::inject-metadata
 (fn [db [_ title handler]]
   (let [result-meta (js/document.querySelector
                      (str "meta[title=" title "]"))
         string-content (.getAttribute result-meta "content")
         edn-content (read-string string-content)]
     (assoc db handler edn-content))))

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
 ::submit-query-box
 (fn [{:keys [db]} [_ query-value]]
   (let [query-map (read-string query-value)
         query-params (cast-to-query-params query-map)]
     {:http-xhrio {:method :get
                   :uri (str "/_query?" query-params)
                   :response-format (ajax-edn/edn-response-format)
                   :on-success [::success-submit-query-box query-params (:find query-map)]
                   :on-failure [::fail-submit-query-box]}})))

(rf/reg-event-fx
 ::success-submit-query-box
 (fn [{:keys [db]} [_ query-params find-clause result]]
   (prn "submit query box success!")
   {:db (assoc db :query-data
               (assoc result "find-clause" find-clause))
    :dispatch [:navigate {:page :query
                          :query-params query-params}]}))

(rf/reg-event-db
 ::fail-submit-query-box (fn [db [_ result]]
   (prn "Failure: get query result: " result)
   db))
