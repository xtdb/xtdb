(ns crux.http-server.get-swagger
  (:require [crux.http-server :as chs]))

(defn -main []
  (let [handler (chs/->crux-handler nil nil)]
    (->> (handler {:request-method :get
                   :uri "/_crux/swagger.json"
                   :query-params {}})
         :body
         (slurp)
         (spit "swagger.json"))))
