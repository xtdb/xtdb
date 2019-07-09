(ns juxt.crux-lib.http-functions
  (:require [promesa.core :as p]))


(defn fetch [{:keys [method url] :as opts}]
  (assert (#{nil :post :get} (:method opts)) (str "Unsupported HTTP method: " (:method opts)))
  (p/alet [fp (js/fetch url (-> opts (update :method (fnil name :get)) clj->js))
           resp (p/await fp)
           headers (.-headers resp)
           content-type (.get headers "Content-Type")
           text (p/await (.text resp))]
    {:body text
     :status (.-status resp)
     :headers {:content-type content-type}}))

