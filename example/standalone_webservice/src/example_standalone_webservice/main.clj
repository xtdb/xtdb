(ns example-standalone-webservice.main
  (:require [crux.api :as crux]
            [yada.yada :refer [handler listener]]
            [hiccup2.core :refer [html]]
            [yada.resource :refer [resource]]))

(defn get-handler
  [ctx {:keys [crux]}]
  (fn [ctx]
    (str
      (html
        [:div
         [:h1 "Message Board"]
         [:br]
         [:form {:action "" :method "POST"}
          [:label "Name: "] [:br]
          [:input {:type "text" :name "name"}] [:br]
          [:br]
          [:label "Message: "] [:br]
          [:textarea {:cols "40" :rows "10" :name "message"}] [:br]
          [:input {:type "submit" :value "Submit"}]]

         [:br]
         [:div
          [:ul
           (for [[message name created]
                 (sort-by #(nth % 2)
                          (.q (.db crux)
                              '{:find [m n c]
                                :where [[e :message-post/message m]
                                        [e :message-post/name n]
                                        [e :message-post/created c]]}))]
             [:li
              [:span "From: " name] [:br]
              [:span "Created: " created] [:br]
              [:pre message]])]]]))))


(defn post-handler
  [ctx {:keys [crux]}]
  (let [{:keys [name message]} (:form (:parameters ctx))]
    (let [id (java.util.UUID/randomUUID)]
      (.submitTx
        crux
        [[:crux.tx/put
          id
          {:crux.db/id id
           :message-post/created (java.util.Date.)
           :message-post/name name
           :message-post/message message}]]))

    (assoc (:response ctx)
           :status 302
           :headers {"location" "/"})))


(defn application-resource
  [system]
  (resource
    {:methods
     {:get {:produces "text/html"
            :response #(get-handler % system)}
      :post {:consumes "application/x-www-form-urlencoded"
             :parameters {:form {:name String
                                 :message String}}
             :produces "text/html"
             :response #(post-handler % system)}}}))

(defn -main []
  (try
    (with-open [crux-system (crux/start-standalone-system
                              {:kv-backend "crux.kv.rocksdb.RocksKv"
                               :event-log-dir "data-eventlog"
                               :db-dir "data"})]
      (.submitTx
        crux-system
        [[:crux.tx/put :example
          {:crux.db/id :example
           :example-value "hello world"}]])

      (let [port 8080]
        (println "started webserver on port:" port)
        (listener
          (application-resource {:crux crux-system})
          {:port port}))

      (.join (Thread/currentThread)))
    (catch Exception e
      (println "what happened" e))))
