(ns tmt.client
  (:require [crux.api :as api]
            [crux.remote-api-client :as cli]))

(def node-http
  (cli/new-api-client "http://localhost:7778"))

(api/status node-http)

(api/q (api/db node-http) '{:find e
                            :where [[_ :crux.db/id e]]})
