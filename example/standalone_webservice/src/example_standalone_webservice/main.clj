(ns example-standalone-webservice.main
  (:require [crux.api :as crux]))

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

      (println
        "example query result: "
        (.q (.db crux-system)
            '{:find [v]
              :where [[e :example-value v]]})))

       (catch Exception e
         (println "what happened" e))))
