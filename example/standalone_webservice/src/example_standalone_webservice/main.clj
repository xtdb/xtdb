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
         [:h1 "hello world"]
         [:br]
         [:div
          "query-result: " (pr-str (.q (.db crux)
                                       '{:find [v]
                                         :where [[e :example-value v]]}))]]))))

(defn application-resource
  [system]
  (resource
    {:methods
     {:get {:produces "text/html"
            :response #(get-handler % system)}}}))

(defn -main []
  (try
    (with-open [crux-system (crux/start-standalone-system
                              {:kv-backend "crux.kv.rocksdb.RocksKv"
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
