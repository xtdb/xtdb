(ns crux.soak.main
  (:require [ring.util.response :as ring]
            [ring.adapter.jetty :as jetty]))

(def server-id (java.util.UUID/randomUUID))

(defn handler [request]
  (-> (ring/response (str {:server-id server-id :body (slurp (:body request))}))
      (ring/content-type "text/plain")))

(defn -main [& _]
  (jetty/run-jetty handler {:port 8080}))
