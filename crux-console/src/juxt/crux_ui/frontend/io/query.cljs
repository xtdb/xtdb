(ns juxt.crux-ui.frontend.io.query
  (:require [clojure.core.async :as async
             :refer [take! put! <! >! timeout chan alt! go go-loop]]
            [re-frame.core :as rf]
            [juxt.crux-lib.async-http-client :as crux-api]
            [promesa.core :as p]))

(defn post-opts [body]
  #js {:method "POST"
       :body body
       :headers #js {:Content-Type "application/edn"}})

(def node-client (crux-api/new-api-client "http://localhost:8080"))

(defn on-exec-success [resp]
  (rf/dispatch [:evt.io/query-success resp]))

(defn on-stats-success [resp]
  (rf/dispatch [:evt.io/stats-success resp]))

(defn on-tx-success [resp]
  (rf/dispatch [:evt.io/tx-success resp]))

(defn on-histories-success [resp]
  (rf/dispatch [:evt.io/histories-fetch-success resp]))

(defn submit-tx []
  (let [tx [[:crux.tx/put :dbpedia.resource/Pablo-Picasso3 ; id for Kafka
             {:crux.db/id :dbpedia.resource/Pablo-Picasso3 ; id for Crux
              :name "Pablo"
              :last-name "Picasso3"}]]
        promise (crux-api/submitTx node-client tx)]
    (.then on-tx-success)))

(defn exec-q [query-text vt tt]
  (let [db (crux-api/db node-client vt tt)
        promise (crux-api/q db query-text)]
    (.then promise on-exec-success)))

(defn exec-tx [query-text]
  (-> node-client
      (crux-api/submitTx query-text)
      (p/then on-tx-success)))

(defn exec [{:keys [query-vt query-tt raw-input query-analysis] :as query}]
  (let [qtype (:crux.ui/query-type query-analysis)]
    (if (false? query-analysis)
      (println "err") ; TODO feedback to UI, or rather, UI should let it get this far
      (case qtype
        :crux.ui.query-type/query     (exec-q raw-input query-vt query-tt)
        :crux.ui.query-type/tx-multi  (exec-tx raw-input)
        :crux.ui.query-type/tx-single (exec-tx raw-input)))))

(defn fetch-stats []
  (-> node-client
      (crux-api/attributeStats)
      (p/then on-stats-success)))

(defn fetch-history [eid]
  (crux-api/historyRange node-client eid nil nil nil nil))

(fetch-history :ids/fashion-ticker-2)

(defn fetch-histories [eids]
  (-> (p/all (map fetch-history eids))
      (p/then on-histories-success)))

(comment
  (exec-q (pr-str '{:full-results? true
                    :find [e]
                    :where [[e :name "Pablo"]]})))
