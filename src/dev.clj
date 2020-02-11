(ns dev
  (:require [clojure.java.io :as io]
            [crux-ui-server.main :as main]))

(main/stop-servers)
(main/-main)

(comment
  (io/resource "/static/crux-ui/compiled/main.js")
  (main/handler {:uri "/static/crux-ui/compiled/main.js"}))
