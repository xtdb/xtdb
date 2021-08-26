(ns xtdb.http-server.get-swagger
  (:require [xtdb.http-server :as chs]))

(defn -main []
  (let [handler (chs/->crux-handler nil nil)]
    (->> (handler {:request-method :get
                   :uri "/_xtdb/swagger.json"
                   :query-params {}})
         :body
         (slurp)
         (spit "swagger.json"))))
