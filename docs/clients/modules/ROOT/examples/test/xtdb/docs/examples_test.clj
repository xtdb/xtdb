(ns xtdb.docs.examples-test
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]))

;; tag::start-http-client[]
(defn start-http-client [port]
  (xt/new-api-client (str "http://localhost:" port)))
;; end::start-http-client[]
