(ns crux.ui.events
  (:require
   [ajax.edn :as ajax-edn]
   [cljs.reader :refer [read-string]]
   [clojure.string :as string]
   [day8.re-frame.http-fx]
   [re-frame.core :as rf]
   [tick.alpha.api :as t]))

(rf/reg-event-fx
 ::inject-metadata
 (fn [{:keys [db]} [_ title handler]]
   (let [result-meta (some-> (js/document.querySelector
                              (str "meta[title=" title "]"))
                             (.getAttribute "content"))
         edn-content (read-string result-meta)]
     (if edn-content
       {:db (assoc db handler edn-content)}
       (js/console.warn "Metadata not found")))))

(defn cast-to-query-params
  [query valid-time transaction-time]
  (let [{:keys [find where limit
                offset args order-by]} query]
    (str
     (cond-> (js/URLSearchParams.)
       :find (doto (.append "find" find))
       :where ((fn [params] (reduce (fn [params clause] (doto params (.append "where" clause))) params where)))
       limit (doto (.append "limit" limit))
       offset (doto (.append "offset" offset))
       args (doto (.append "args" args))
       order-by ((fn [params] (reduce (fn [params clause] (doto params (.append "order-by" clause))) params order-by)))
       valid-time (doto (.append "valid-time" valid-time))
       transaction-time (doto (.append "transaction-time" transaction-time))))))

(rf/reg-event-fx
 ::go-to-query-table
 (fn [{:keys [db]} [_ {:keys [values]}]]
   (let [{:strs [valid-date valid-time transaction-date transaction-time]} values
         query-map (read-string (get values "q"))
         parsed-valid-time (when (and valid-date valid-time)
                             (t/instant (str valid-date  "T" valid-time)))
         parsed-transaction-time (when (and transaction-date transaction-time)
                                   (t/instant (str transaction-date  "T"
                                                   transaction-time)))
         query-params (cast-to-query-params query-map parsed-valid-time parsed-transaction-time)]
     {:dispatch [:navigate {:page :query
                            :query-params query-params}]})))

(rf/reg-event-fx
 ::prev-next-links
 (fn [{:keys [db]} [_ query-params]]
   {:dispatch [:navigate {:page :query
                          :query-params query-params}]}))

(rf/reg-event-fx
 ::fetch-query-table
 (fn [{:keys [db]} _]
   (let [query-params (doto (js/URLSearchParams. js/window.location.search) (.delete "full-results"))
         find (read-string (.get query-params "find"))
         link-entities? (.get query-params "link-entities?")]
     (when (seq (str query-params))
       {:http-xhrio {:method :get
                     :uri (str "/_query?" query-params (when-not link-entities?
                                                         "&link-entities?=true"))
                     :response-format (ajax-edn/edn-response-format)
                     :on-success [::success-fetch-query-table find]
                     :on-failure [::fail-fetch-query-table]}}))))

(rf/reg-event-fx
 ::success-fetch-query-table
 (fn [{:keys [db]} [_ find-clause result]]
   (prn "fetch query table success!")
   {:db (assoc db :query-data
               (assoc result "find-clause" find-clause))}))

(rf/reg-event-db
 ::fail-fetch-query-table
 (fn [db [_ result]]
   (prn "Failure: get query table result: " result)
   (assoc-in db [:query-data :error]
             (get-in result [:response :via 0 :message]))))

(rf/reg-event-fx
 ::fetch-entity
 (fn [{:keys [db]} _]
   (let [entity-id (-> js/window.location.pathname
                       (string/split #"/")
                       last)
         query-params (js/URLSearchParams. js/window.location.search)
         link-entities? (.get query-params "link-entities?")]
     {:http-xhrio {:method :get
                   :uri (str "/_entity/" entity-id "?" query-params
                             (when-not link-entities? "&link-entities?=true"))
                   :response-format (ajax-edn/edn-response-format)
                   :on-success [::success-fetch-entity]
                   :on-failure [::fail-fetch-entity]}})))

(rf/reg-event-fx
 ::success-fetch-entity
 (fn [{:keys [db]} [_ result]]
   (prn "fetch entity success!")
   {:db (assoc db :entity-data result)}))

(rf/reg-event-db
 ::fail-fetch-entity
 (fn [db [_ {:keys [message] :as result}]]
   (prn "Failure: get fetch entity result: " result)
   db))
