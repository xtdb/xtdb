(ns webservice.ds9
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [crux.api :as c]
            [integrant.core :as ig]))

(defn populate
  [node dataset]
  (c/sync node
          (:crux.tx/tx-time
            (c/submit-tx node (mapv (fn [d] [:crux.tx/put d])
                                    (:data dataset))))
          nil))

(defmethod ig/init-key ::populate
  [_ {:keys [webservice/crux-node webservice.ds9/dataset]}]
  (populate crux-node
            (edn/read-string
              (slurp (io/resource dataset)))))
