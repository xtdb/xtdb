(ns xtdb.ui.events
  (:require [ajax.edn :as ajax-edn]
            [cljs.reader :as reader]
            [clojure.string :as string]
            [xtdb.ui.common :as common]
            [xtdb.ui.http]
            [re-frame.core :as rf]
            [tick.alpha.api :as t]
            [xtdb.http-server.entity-ref :as entity-ref]))

(rf/reg-fx
 :scroll-top
 common/scroll-top)

(rf/reg-event-fx
 ::inject-metadata
 (fn [{:keys [db]} [_ title handler]]
   (let [result-meta (some-> (js/document.querySelector
                              (str "meta[title=" title "]"))
                             (.getAttribute "content"))
         edn-content (reader/read-string {:readers {'object pr-str
                                                    'xt/id str
                                                    'xt.http/entity-ref entity-ref/->EntityRef}} result-meta)]
     (if edn-content
       {:db (assoc db handler edn-content)}
       (js/console.warn "Metadata not found")))))

(rf/reg-event-db
 ::navigate-to-root-view
 (fn [db [_ view]]
   (-> (assoc-in db [:form-pane :hidden?] false)
       (assoc-in [:form-pane :view] view))))

(rf/reg-event-db
 ::toggle-query-form
 (fn [db [_ visible?]]
   (update-in db [:query-form :visible?] (if (some? visible?) (constantly visible?) not))))

(rf/reg-event-db
 ::query-form-tab-selected
 (fn [db [_ tab]]
   (assoc-in db [:query-form :selected-tab] tab)))

(rf/reg-event-fx
 ::console-tab-selected
 (fn [{:keys [db]} [_ tab]]
   (let [current-tab (get-in db [:current-route :data :name] :query)
         current-params (get-in db [:current-route :query-params])]
     (when (not= tab current-tab)
       {:db (-> (assoc-in db [:previous-params current-tab] current-params)
                (assoc :load-from-state? true))
        :dispatch [:navigate tab {} (get-in db [:previous-params tab])]}))))

(rf/reg-event-db
 ::status-tab-selected
 (fn [db [_ tab]]
   (assoc-in db [:status :selected-tab] tab)))

(rf/reg-event-db
 ::toggle-form-pane
 (fn [db [_ & bool]]
   (update-in db [:form-pane :hidden?] #(if (seq bool) (first bool) (not %)))))

(rf/reg-event-db
 ::toggle-form-history
 (fn [db [_ component & bool]]
   (update-in db [:form-pane :history component] #(if (seq bool) (first bool) (not %)))))

(rf/reg-event-db
 ::toggle-show-vt
 (fn [db [_ component bool]]
   (update-in db [:form-pane :show-vt? component] #(if (nil? %) (not bool) (not %)))))

(rf/reg-event-db
 ::toggle-show-tt
 (fn [db [_ component bool]]
   (update-in db [:form-pane :show-tt? component] #(if (nil? %) (not bool) (not %)))))

(rf/reg-event-db
 ::set-form-pane-view
 (fn [db [_ view]]
   (assoc-in db [:form-pane :view] view)))

(rf/reg-event-db
 ::query-table-error
 (fn [db [_ error]]
   (assoc-in db [:query :error] error)))

(rf/reg-event-db
 ::set-query-result-pane-loading
 (fn [db [_ bool]]
   (assoc-in db [:query :result-pane :loading?] bool)))

(rf/reg-event-fx
 ::inject-local-storage
 (fn [{:keys [db]} _]
   {:db (-> db
            (assoc :query-history (reader/read-string
                                   (.getItem js/window.localStorage "query"))))}))

(defn vec-remove
  [pos coll]
  (vec (concat (subvec coll 0 pos) (subvec coll (inc pos)))))

(rf/reg-event-fx
 ::remove-query-from-local-storage
 (fn [{:keys [db]} [_ idx]]
   (let [query-history (:query-history db)
         updated-history (vec-remove idx query-history)]
     {:db (assoc db :query-history updated-history)
      :local-storage ["query" updated-history]})))

(rf/reg-fx
 :local-storage
 (fn [[k data]]
   (.setItem js/window.localStorage k data)))

(rf/reg-event-fx
 ::go-to-query-view
 (fn [{:keys [db]} [_ {:keys [values]}]]
   (let [{:strs [q valid-time tx-time]} values
         query-params (->> {:query-edn (reader/read-string q)
                            :valid-time (some-> valid-time js/moment .toDate t/instant)
                            :tx-time (some-> tx-time js/moment .toDate t/instant)}
                           (remove #(nil? (second %)))
                           (into {}))
         history-elem (dissoc query-params :valid-time :tx-time)
         current-storage (or (reader/read-string (.getItem js/window.localStorage "query")) [])
         updated-history (conj (into [] (remove #(= history-elem %) current-storage)) history-elem)]
     {:db (assoc db :query-history updated-history)
      :dispatch [:navigate :query {} query-params]
      :local-storage ["query" updated-history]})))

(rf/reg-event-fx
 ::goto-previous-query-page
 (fn [{:keys [db]} _]
   (let [query-params (get-in db [:current-route :query-params])
         offset (js/parseInt (get-in db [:current-route :query-params :offset] 0))
         limit (js/parseInt (get-in db [:current-route :query-params :limit] 100))]
     {:db db
      :dispatch [:navigate :query {} (assoc query-params :offset (-> (- offset limit)
                                                                     (max 0)
                                                                     str))]})))

(rf/reg-event-fx
 ::goto-next-query-page
 (fn [{:keys [db]} _]
   (let [query-params (get-in db [:current-route :query-params])
         offset (js/parseInt (get-in db [:current-route :query-params :offset] 0))
         limit (js/parseInt (get-in db [:current-route :query-params :limit] 100))]
     {:db db
      :dispatch [:navigate :query {} (assoc query-params :offset (-> (+ offset limit) str))]})))

(rf/reg-event-fx
 ::go-to-historical-query
 (fn [{:keys [db]} [_ history-q]]
   (let [{:strs [q]} history-q
         query-params (->>
                       (common/edn->query-params (reader/read-string q))
                       (remove #(nil? (second %)))
                       (into {}))]
     {:dispatch [:navigate :query {} query-params]})))

(rf/reg-event-fx
 ::entity-pane-tab-selected
 (fn [{:keys [db]} [_ tab]]
   {:dispatch [:navigate :entity nil (case tab
                                       :document (-> (get-in db [:current-route :query-params])
                                                     (select-keys [:valid-time :tx-time :eid-edn]))
                                       :history (-> (get-in db [:current-route :query-params])

                                                    (assoc :history true)
                                                    (assoc :with-docs true)
                                                    (assoc :sort-order "desc")))]}))

(rf/reg-event-db
 ::set-entity-result-pane-loading
 (fn [db [_ bool]]
   (assoc-in db [:entity :result-pane :loading?] bool)))

(rf/reg-event-fx
 ::go-to-entity-view
 (fn [{:keys [db]} [_ {{:strs [eid valid-time tx-time]} :values}]]
   (let [query-params (->>
                       {:valid-time (some-> valid-time js/moment .toDate t/instant)
                        :tx-time (some-> tx-time js/moment .toDate t/instant)
                        :eid-edn eid}
                       (remove #(nil? (second %)))
                       (into {}))]
     {:db db
      :dispatch [:navigate :entity nil query-params]})))

(rf/reg-event-db
 ::entity-result-pane-document-error
 (fn [db [_ error]]
   (assoc-in db [:entity :error] error)))

(rf/reg-event-db
 ::set-node-status-loading false
 (fn [db [_ bool]]
   (assoc-in db [:status :loading?] bool)))

(rf/reg-event-db
 ::set-node-attribute-stats-loading false
 (fn [db [_ bool]]
   (assoc-in db [:attribute-stats :loading?] bool)))
